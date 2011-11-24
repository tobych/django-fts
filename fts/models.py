"""
Full Text Search Framework
"""
import datetime
from django.db import models
from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes import generic
from django.core.cache import cache

from fts.settings import *

if FTS_CONFIGURE_ALL_BACKENDS or FTS_BACKEND.startswith('simple://'):
    class Word(models.Model):
        word = models.CharField(unique=True, db_index=True, blank=False, max_length=100)
        
        def __unicode__(self):
            return u"%s" % (self.word)
    
    class Namespace(models.Model):
        slug = models.SlugField()

        def save(self, *args, **kwargs):
            super(Namespace, self).save(*args, **kwargs)

            # store section's schema modification time:
            key = "fts-namespaces-last-updated"
            cache.set(key, datetime.datetime.now())

        def delete(self):
            super(Namespace, self).delete()

            # store section's schema modification time:
            key = "fts-namespaces-last-updated"
            cache.set(key, datetime.datetime.now())

        def __unicode__(self):
            return u"%s" % (self.slug)

    class Index(models.Model):
        word = models.ForeignKey(Word)
        weight = models.IntegerField()

        namespace = models.ForeignKey(Namespace, null=True, blank=True)
        
        content_type = models.ForeignKey(ContentType)
        object_id = models.PositiveIntegerField(db_index=True)
        content_object = generic.GenericForeignKey('content_type', 'object_id')
        
        def __unicode__(self):
            return u'%s [%s]' % (self.content_object, self.word.word)
