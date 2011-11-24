__all__ = ('backend', 'SearchableModel', 'SearchableManager',
           'SimpleSearchableModel', 'SimpleSearchableManager',
           'DummySearchableModel', 'DummySearchableManager',
           'MysqlSearchableModel', 'MysqlSearchableManager',
           'PgsqlSearchableModel', 'PgsqlSearchableManager',
           'SphinxSearchableModel', 'SphinxSearchableManager',
           'XapianSearchableModel', 'XapianSearchableManager')

from cgi import parse_qsl
from django.core import signals
from fts.backends.base import InvalidFtsBackendError

from fts.settings import *

# Name for use in settings file --> name of module in "backends" directory.
# Any backend scheme that is not in this dictionary is treated as a Python
# import path to a custom backend.
BACKENDS = {
    'pgsql': 'pgsql',
    'mysql': 'mysql',
    'sphinx': 'sphinx',
    'xapian': 'xapian',
    'simple': 'simple',
    'dummy': 'dummy',
}

def get_fts(backend_uri):
    if backend_uri.find(':') == -1:
        raise InvalidFtsBackendError, "Backend URI must start with scheme://"
    scheme, rest = backend_uri.split(':', 1)
    if not rest.startswith('//'):
        raise InvalidFtsBackendError, "Backend URI must start with scheme://"

    host = rest[2:]
    qpos = rest.find('?')
    if qpos != -1:
        params = dict(parse_qsl(rest[qpos+1:]))
        host = rest[2:qpos]
    else:
        params = {}
    if host.endswith('/'):
        host = host[:-1]

    if scheme in BACKENDS:
        module = __import__('fts.backends.%s' % BACKENDS[scheme], {}, {}, [''])
    else:
        module = __import__(scheme, {}, {}, [''])
    return getattr(module, 'SearchClass')(host, params), getattr(module, 'SearchableModel'), getattr(module, 'SearchManager')

SearchableModel, SearchManager = None, None
SimpleSearchableModel, SimpleSearchManager = None, None
DummySearchableModel, DummySearchManager = None, None
MysqlSearchableModel, MysqlSearchManager = None, None
PgsqlSearchableModel, PgsqlSearchManager = None, None
SphinxSearchableModel, SphinxSearchManager = None, None
XapianSearchableModel, XapianSearchManager = None, None

if FTS_CONFIGURE_ALL_BACKENDS or FTS_BACKEND.startswith('simple://'):
    try:
        _fts, SimpleSearchableModel, SimpleSearchManager = get_fts('simple://')
        if FTS_BACKEND.startswith('simple://'):
            SearchableModel, SearchManager = SimpleSearchableModel, SimpleSearchManager
            backend = _fts.backend
    except InvalidFtsBackendError:
        if FTS_BACKEND.startswith('simple://'):
            raise

if FTS_CONFIGURE_ALL_BACKENDS or FTS_BACKEND.startswith('dummy://'):
    try:
        _fts, DummySearchableModel, DummySearchManager = get_fts('dummy://')
        if FTS_BACKEND.startswith('dummy://'):
            SearchableModel, SearchManager = DummySearchableModel, DummySearchManager
            backend = _fts.backend
    except InvalidFtsBackendError:
        if FTS_BACKEND.startswith('dummy://'):
            raise
    
if FTS_CONFIGURE_ALL_BACKENDS or FTS_BACKEND.startswith('mysql://'):
    try:
        _fts, MysqlSearchableModel, MysqlSearchManager = get_fts('mysql://')
        if FTS_BACKEND.startswith('mysql://'):
            SearchableModel, SearchManager = MysqlSearchableModel, MysqlSearchManager
            backend = _fts.backend
    except InvalidFtsBackendError:
        if FTS_BACKEND.startswith('mysql://'):
            raise
    
if FTS_CONFIGURE_ALL_BACKENDS or FTS_BACKEND.startswith('pgsql://'):
    try:
        _fts, PgsqlSearchableModel, PgsqlSearchManager = get_fts('pgsql://')
        if FTS_BACKEND.startswith('pgsql://'):
            SearchableModel, SearchManager = PgsqlSearchableModel, PgsqlSearchManager
            backend = _fts.backend
    except InvalidFtsBackendError:
        if FTS_BACKEND.startswith('pgsql://'):
            raise
    
if FTS_CONFIGURE_ALL_BACKENDS or FTS_BACKEND.startswith('sphinx://'):
    try:
        _fts, SphinxSearchableModel, SphinxSearchManager = get_fts('sphinx://')
        if FTS_BACKEND.startswith('sphinx://'):
            SearchableModel, SearchManager = SphinxSearchableModel, SphinxSearchManager
            backend = _fts.backend
    except InvalidFtsBackendError:
        if FTS_BACKEND.startswith('sphinx://'):
            raise
    
if FTS_CONFIGURE_ALL_BACKENDS or FTS_BACKEND.startswith('xapian://'):
    try:
        _fts, XapianSearchableModel, XapianSearchManager = get_fts('xapian://')
        if FTS_BACKEND.startswith('xapian://'):
            SearchableModel, SearchManager = XapianSearchableModel, XapianSearchManager
            backend = _fts.backend
    except InvalidFtsBackendError:
        if FTS_BACKEND.startswith('xapian://'):
            raise
