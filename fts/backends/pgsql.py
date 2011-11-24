"Pgsql Fts backend"

from django.db import connection, transaction
from django.db.models.fields import FieldDoesNotExist

from fts.backends.base import InvalidFtsBackendError
from fts.backends.base import BaseClass, BaseModel, BaseManager

qn = connection.ops.quote_name

from django.db import models
LANGUAGES = {
    '' : 'simple',
    'da' : 'danish',
    'nl' : 'dutch',
    'en' : 'english',
    'fi' : 'finnish',
    'fr' : 'french',
    'de' : 'german',
    'hu' : 'hungarian',
    'it' : 'italian',
    'no' : 'norwegian',
    'pt' : 'portuguese',
    'ro' : 'romanian',
    'ru' : 'russian',
    'es' : 'spanish',
    'sv' : 'swedish',
    'tr' : 'turkish',
}

class VectorField(models.Field):
    def __init__(self, *args, **kwargs):
        kwargs['null'] = True
        kwargs['editable'] = False
        kwargs['serialize'] = False
        super(VectorField, self).__init__(*args, **kwargs)
    
    def db_type(self, connection=None):
        return 'tsvector'

class SearchClass(BaseClass):
    def __init__(self, server, params):
        from django.conf import settings
        
        if not settings.DATABASE_ENGINE in ['postgresql', 'postgresql_psycopg2']:
            raise InvalidFtsBackendError("PostgreSQL with tsearch2 support is needed to use the pgsql FTS backend")
        
        self.backend = 'pgsql'

class SearchManager(BaseManager):
    def __init__(self, **kwargs):
        super(SearchManager, self).__init__(**kwargs)
        self.language = LANGUAGES[self.language_code]
        self._vector_field_cache = None

    def _vector_field(self):
        """
        Returns the VectorField defined for this manager's model. There must be exactly one VectorField defined.
        """
        if self._vector_field_cache is not None:
            return self._vector_field_cache
        
        vectors = [f for f in self.model._meta.fields if isinstance(f, VectorField)]
        
        if len(vectors) != 1:
            raise ValueError('There must be exactly 1 VectorField defined for the %s model.' % self.model._meta.object_name)
            
        self._vector_field_cache = vectors[0]
        
        return self._vector_field_cache
    vector_field = property(_vector_field)
    
    def _vector_sql(self, field, weight):
        """
        Returns the SQL used to build a tsvector from the given (django) field name.
        """
        try:
            f = self.model._meta.get_field(field)
            return ("setweight(to_tsvector('%s', coalesce(%s,'')), '%s')" % (self.language, qn(f.column), weight), [])
        except FieldDoesNotExist:
            return ("setweight(to_tsvector('%s', %%s), '%s')" % (self.language, weight), [field])

    def _update_index_update(self, pk=None):
        # Build a list of SQL clauses that generate tsvectors for each specified field.
        clauses = []
        params = []
        for field, weight in self._fields.items():
            v = self._vector_sql(field, weight)
            clauses.append(v[0])
            params.extend(v[1])
        vector_sql = ' || '.join(clauses)
        
        where = ''
        # If one or more pks are specified, tack a WHERE clause onto the SQL.
        if pk is not None:
            if isinstance(pk, (list,tuple)):
                ids = ','.join(str(v) for v in pk)
                where = ' WHERE %s IN (%s)' % (qn(self.model._meta.pk.column), ids)
            else:
                where = ' WHERE %s = %d' % (qn(self.model._meta.pk.column), pk)
        sql = 'UPDATE %s SET %s = %s%s' % (qn(self.model._meta.db_table), qn(self.vector_field.column), vector_sql, where)
        cursor = connection.cursor()
        cursor.execute(sql, tuple(params))
        transaction.set_dirty()

    def _update_index_walking(self, pk=None):
        if pk is not None:
            if isinstance(pk, (list,tuple)):
                items = self.filter(pk__in=pk)
            else:
                items = self.filter(pk=pk)
        else:
            items = self.all()
        
        IW = {}
        for item in items:
            clauses = []
            params = []
            for field, weight in self._fields.items():
                if callable(field):
                    words = field(item)
                elif '__' in field:
                    words = item
                    for col in field.split('__'):
                        words = getattr(words, col)
                else:
                    words = field
                v = self._vector_sql(words, weight)
                clauses.append(v[0])
                params.extend(v[1])
            vector_sql = ' || '.join(clauses)
            sql = 'UPDATE %s SET %s = %s WHERE %s = %d' % (qn(self.model._meta.db_table), qn(self.vector_field.column), vector_sql, qn(self.model._meta.pk.column), item.pk)
            cursor = connection.cursor()
            cursor.execute(sql, tuple(params))
        transaction.set_dirty()
    
    @transaction.commit_on_success
    def _update_index(self, pk=None):
        index_walking = False
        for field, weight in self._fields.items():
            if callable(field) or '__' in field:
                index_walking = True
                break
        if index_walking:
            self._update_index_walking(pk)
        else:
            self._update_index_update(pk)
    
    def _search(self, query, **kwargs):
        """
        Returns a queryset after having applied the full-text search query. If rank_field
        is specified, it is the name of the field that will be put on each returned instance.
        When specifying a rank_field, the results will automatically be ordered by -rank_field.
        
        For possible rank_normalization values, refer to:
        http://www.postgresql.org/docs/8.3/static/textsearch-controls.html#TEXTSEARCH-RANKING
        """
        rank_field = kwargs.get('rank_field')
        rank_normalization = kwargs.get('rank_normalization', 32)
        qs = self.get_query_set()
        
        ts_query = "plainto_tsquery('%s','%s')" % (self.language, unicode(query).replace("'", "''"))
        where = '%s.%s @@ %s' % (qn(self.model._meta.db_table), qn(self.vector_field.column), ts_query)
        
        select = {}
        order = []
        if rank_field is not None:
            select[rank_field] = 'ts_rank(%s.%s, %s, %d)' % (qn(self.model._meta.db_table), qn(self.vector_field.column), ts_query, rank_normalization)
            order = ['-%s' % rank_field]
        
        return qs.extra(select=select, where=[where], order_by=order)

class SearchableModel(BaseModel):
    class Meta:
        abstract = True

    search_index = VectorField()

    objects = SearchManager()
