"Simple Fts backend"
import re
import os
import datetime

from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes import generic
from django.db import connection, transaction
from django.db.models import Q, Max
from django.core.cache import cache

from snippets.decorators import commit_on_success_unless_managed

from fts.backends.base import BaseClass, BaseModel, BaseManager
from fts.models import Word, Index, Namespace

import unicodedata
from fts.words.stop import FTS_STOPWORDS
try:
    from fts.words.snowball import Stemmer
except ImportError:
    from fts.words.porter import Stemmer

qn = connection.ops.quote_name

WEIGHTS = {
    'A' : 10,
    'B' : 4,
    'C' : 2,
    'D' : 1
}
SEP = re.compile(r'[\s,.()\[\]|]')

_NAMESPACES_CACHE = {}
_NAMESPACES_CACHE_SYNC = {}

class SearchClass(BaseClass):
    def __init__(self, server, params):
        self.backend = 'simple'

class SearchManager(BaseManager):
    def __init__(self, **kwargs):
        super(SearchManager, self).__init__(**kwargs)
        # For autocomplete, generally you'd want:
        #   full_index=True and stem_words=False (full_index implies exact_search)
        # For regular Fulltext search, you'd want:
        #   full_index=False, steam_words=True and exact_search=True
        self.full_index = kwargs.get('full_index', False)
        self.stem_words = kwargs.get('stem_words', True)
        self.exact_search = kwargs.get('exact_search', True)
        self.namespace = kwargs.get('namespace', None)

    def _get_namespace_id(self, namespace):
        _k_ = namespace
        try:
            now = datetime.datetime.now()
            sync_time = _NAMESPACES_CACHE_SYNC.get(_k_)
            expired = not sync_time and True or False
            keys = (
                'fts-namespaces-last-updated',
            )
            for key in keys:
                if not key: continue
                last_updated = cache.get(key)
                if not last_updated:
                    last_updated = now
                    cache.set(key, last_updated)
                if sync_time and last_updated > sync_time:
                    expired = True
            if expired:
                raise KeyError
            namespace_id = _NAMESPACES_CACHE[_k_]
        except KeyError:
            for n in Namespace.objects.all():
                _NAMESPACES_CACHE[n.slug] = n.id

            namespace_id = _NAMESPACES_CACHE.get(namespace)

            # save sync time for cache:
            _NAMESPACES_CACHE[_k_] = namespace_id
            _NAMESPACES_CACHE_SYNC[_k_] = datetime.datetime.now()

        return namespace_id

    def _get_idx_words(self, line, minlen=0):
        words = self._get_words(line, minlen)
        if self.full_index:
            # Find all the substrings of the word (all digit words treated differently):
            words = set( word[i:j] for word in words for i in not word.isdigit() and range(len(word)) or (0,) for j in range(i+1, len(word)+1) if j-i > minlen )
        return words
    
    def _get_words(self, line, minlen=0):
        # Remove accents
        line = ''.join((c for c in unicodedata.normalize('NFD', unicode(line)) if unicodedata.category(c) != 'Mn'))
        # Lowercase and split in a set of words
        words = set(SEP.split(line.lower()))
        # Stemmer function
        if self.stem_words:
            stem = Stemmer(self.language_code)
        else:
            stem = lambda w: w
        # Get stemmed set of words not in the list of stop words and with a minimum of a minlen length
        return set( stem(word) for word in words if word and word not in FTS_STOPWORDS[self.language_code] and len(word) > minlen )
        
    @commit_on_success_unless_managed
    def _update_index(self, pk, dumping=None):
        """
            Index Update (Live or Dumping)
            For Dumping update (recommended method):
                dumping = {}  # use to pass and keep context for multiple calls
                Entity.autocomplete._update_index(None, dumping)
                GeonameAlternateName.autocomplete._update_index(None, dumping)
                TagLabel.autocomplete._update_index(None, dumping)
                then in Sqlite3:
                    .separator "\t"
                    .import fts_word.txt fts_word
                    .import fts_index.txt fts_index
                ...or in PostgreSQL:
                    COPY fts_word FROM 'fts_word.txt';
                    COPY fts_index FROM 'fts_index.txt';
            For Live update (very slow compared to dumping update):
                TagLabel.autocomplete.update_index()
            Usage:
                TagLabel.autocomplete.search('label')
        """
        if self.model._meta.abstract:
            return # skip abstract class updates
        namespace_id = self._get_namespace_id(self.namespace)
        if not namespace_id and self.namespace:
            ns = Namespace.objects.create(slug=self.namespace)
            namespace_id = ns.id
        ctype = ContentType.objects.get_for_model(self.model)
        filter = { 'content_type__pk': ctype.pk }
        if namespace_id: filter['namespace'] = namespace_id
        if pk is not None:
            if isinstance(pk, (set,list,tuple)):
                filter['object_id__in'] = pk
                items = self.filter(pk__in=pk)
            else:
                filter['object_id'] = pk
                items = self.filter(pk=pk)
        else:
            items = self.all()
        cursor = connection.cursor()
        cursor.execute('DELETE FROM'+str(Index.objects.filter(**filter).query).split('FROM')[1])
        transaction.set_dirty()
        if dumping is None:
            c = { 'IW': {} }
        else:
            c = dumping
            c['fw'] = c.get('fw') or open('fts_word.txt', 'wt')
            c['fi'] = c.get('fi') or open('fts_index.txt', 'wt')
            c['IW'] = c.get('IW')
            if not c['IW']:
                c['IW'] = {}
                c['widx'] = 0
                c['iidx'] = (Index.objects.aggregate(Max('id'))['id__max'] or 0) + 1
                for iw in Word.objects.all():
                    if iw.id > c['widx']:
                        c['widx'] = iw.id
                    c['IW'][iw.word] = iw.id
                c['widx'] += 1
        for item in items:
            item_words = {}
            for field, weight in self._fields.items():
                if callable(field):
                    words = field(item)
                else:
                    words = item
                    for col in field.split('__'):
                        words = getattr(words, col)
                # get all the possible substrings for words
                idx_words = self._get_idx_words(words)
                if dumping is None:
                    # of all those substrings, retrieve the missing ones in our c['IW'] dictionary
                    idx_words_to_get = [w for w in idx_words if w not in c['IW']]
                if len(idx_words_to_get):
                    for iw in Word.objects.filter(word__in=idx_words_to_get):
                            c['IW'][iw.word] = iw
                # finally, for each substring to index, build the index in item_words:
                for word in idx_words:
                    try:
                        iw = c['IW'][word];
                    except KeyError:
                        if dumping is not None:
                            print >>c['fw'], u'\t'.join([unicode(w) or '' for w in (c['widx'], word)]).encode('utf8')
                            iw = c['IW'][word] = c['widx']
                            c['widx'] += 1
                        else:
                        iw = Word.objects.get_or_create(word=word)[0]
                            c['IW'][word] = iw
                    if ord(weight) < ord(item_words.get(iw, 'Z')):
                        item_words[iw] = weight
            for iw, weight in item_words.items():
                if dumping is not None:
                    print >>c['fi'], u'\t'.join([unicode(w) or '' for w in (c['iidx'], iw, WEIGHTS[weight], namespace_id, ctype.pk, item.pk)]).encode('utf8')
                    c['iidx'] += 1
                else:
                    Index.objects.create(content_object=item, word=iw, weight=WEIGHTS[weight], namespace_id=namespace_id)

    def _search(self, query, **kwargs):
        rank_field = kwargs.get('rank_field')
        qs = self.get_query_set()
        
        joins = []
        weights = []
        joins_params = []
        namespace_id = self._get_namespace_id(self.namespace)
        for idx, word in enumerate(self._get_words(query)):
            if self.full_index or self.exact_search:
                joins_params.append("'%s'" % word.replace("'", "''"))
                if namespace_id is not None:
                    joins_params.append(namespace_id)
                    namespace_sql = u'AND i%(idx)d.namespace_id = %%%%d' % { 'idx':idx }
                else:
                    namespace_sql = u''
                joins.append(u"INNER JOIN %%(words_table_name)s AS w%(idx)d ON (w%(idx)d.word = %%%%s) INNER JOIN %%(index_table_name)s AS i%(idx)d ON (w%(idx)d.id = i%(idx)d.word_id AND i%(idx)d.content_type_id = %%(content_type_id)s AND i%(idx)d.object_id = %%(table_name)s.id %(namespace_sql)s)" % { 'idx':idx, 'namespace_sql': namespace_sql })
            else:
                joins_params.append("'%s%%%%'" % word.replace("'", "''"))
                if namespace_id is not None:
                    joins_params.append(namespace_id)
                    namespace_sql = u'AND i%(idx)d.namespace_id = %%%%d' % { 'idx':idx }
                else:
                    namespace_sql = u''
                joins.append(u"INNER JOIN %%(words_table_name)s AS w%(idx)d ON (w%(idx)d.word LIKE %%%%s) INNER JOIN %%(index_table_name)s AS i%(idx)d ON (w%(idx)d.id = i%(idx)d.word_id AND i%(idx)d.content_type_id = %%(content_type_id)s AND i%(idx)d.object_id = %%(table_name)s.id %(namespace_sql)s)" % { 'idx':idx, 'namespace_sql': namespace_sql })
                qs.query.distinct = True
            weights.append("i%(idx)d.weight" % { 'idx':idx })
        
        table_name = self.model._meta.db_table
        words_table_name = qn(Word._meta.db_table)
        index_table_name = qn(Index._meta.db_table)
        
        ctype = ContentType.objects.get_for_model(self.model)
        joins = ' '.join(joins) % {
            'table_name': qn(table_name),
            'words_table_name': words_table_name,
            'index_table_name': index_table_name,
            'content_type_id': ctype.id,
        }
        # these params should be set as FROM params to be returned by get_from_clause() but it doesn't support FROM params
        joins = joins % tuple(joins_params)
        
        # monkey patch the query set:
        qs.query.table_alias(table_name) # create alias
        qs.query.alias_map[table_name] = (table_name, joins, None, None, None, None, None) # map the joins to the alias
        
        if rank_field is not None:
            select = {}
            order = []
            select[rank_field] = '+'.join(weights)
            order = ['-%s' % rank_field]
            qs = qs.extra(select=select, order_by=order)
        
        return qs

class SearchableModel(BaseModel):
    class Meta:
        abstract = True

    objects = SearchManager()
