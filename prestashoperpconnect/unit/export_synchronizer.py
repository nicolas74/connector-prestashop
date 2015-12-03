# -*- coding: utf-8 -*-
##############################################################################
#
#    Copyright (C) 2013 Akretion (http://www.akretion.com).
#    Copyright (C) 2013 Camptocamp (http://www.camptocamp.com)
#    @author Sébastien BEAU <sebastien.beau@akretion.com>
#    @author Guewen Baconnier <guewen.baconnier@camptocamp.com>
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
##############################################################################

import logging
from datetime import datetime
from openerp.tools.translate import _
from openerp.tools import DEFAULT_SERVER_DATETIME_FORMAT
from openerp.addons.connector.queue.job import job, related_action
from openerp.addons.connector.unit.synchronizer import Exporter
from openerp.addons.prestashoperpconnect.unit.mapper import TranslationPrestashopExportMapper
from openerp.addons.connector.exception import (IDMissingInBackend,
                                                RetryableJobError)
from .import_synchronizer import import_record
from ..connector import get_environment
from contextlib import contextmanager
from ..related_action import unwrap_binding

from openerp.addons.prestashoperpconnect.backend import prestashop


_logger = logging.getLogger(__name__)


"""

Exporters for Prestashop.

In addition to its export job, an exporter has to:

* check in Prestashop if the record has been updated more recently than the
  last sync date and if yes, delay an import
* call the ``bind`` method of the binder to update the last sync date

"""


class PrestashopBaseExporter(Exporter):
    """ Base exporter for Prestashop """

    def __init__(self, environment):
        """
        :param environment: current environment (backend, session, ...)
        :type environment: :py:class:`connector.connector.Environment`
        """
        super(PrestashopBaseExporter, self).__init__(environment)
        self.binding_id = None
        self.prestashop_id = None

    def _get_openerp_data(self):
        """ Return the raw OpenERP data for ``self.binding_id`` """
        return self.session.env[self.model._name].browse(self.binding_id)

    def run(self, binding_id, *args, **kwargs):
        """ Run the synchronization

        :param binding_id: identifier of the binding record to export
        """
        self.binding_id = binding_id
        self.erp_record = self._get_openerp_data()
        self.prestashop_id = self.binder.to_backend(self.binding_id)
        result = self._run(*args, **kwargs)

        self.binder.bind(self.prestashop_id, self.binding_id)
        return result

    def _run(self):
        """ Flow of the synchronization, implemented in inherited classes"""
        raise NotImplementedError


class PrestashopExporter(PrestashopBaseExporter):
    """ A common flow for the exports to Prestashop """

    def __init__(self, environment):
        """
        :param environment: current environment (backend, session, ...)
        :type environment: :py:class:`connector.connector.Environment`
        """
        super(PrestashopExporter, self).__init__(environment)
        self.erp_record = None

    def _has_to_skip(self):
        """ Return True if the export can be skipped """
        return False

    @contextmanager
    def _retry_unique_violation(self):
        """ Context manager: catch Unique constraint error and retry the
        job later.

        Taken from connector-magento

        When we execute several jobs workers concurrently, it happens
        that 2 jobs are creating the same record at the same time (binding
        record created by :meth:`_export_dependency`), resulting in:

            IntegrityError: duplicate key value violates unique
            constraint "prestashop_product_product_openerp_uniq"
            DETAIL:  Key (backend_id, openerp_id)=(1, 4851) already exists.

        In that case, we'll retry the import just later.

        .. warning:: The unique constraint must be created on the
                     binding record to prevent 2 bindings to be created
                     for the same Prestashop record.

        """
        try:
            yield
        except psycopg2.IntegrityError as err:
            if err.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
                raise RetryableJobError(
                    'A database error caused the failure of the job:\n'
                    '%s\n\n'
                    'Likely due to 2 concurrent jobs wanting to create '
                    'the same record. The job will be retried later.' % err)
            else:
                raise

    def _export_dependency(self, relation, binding_model, exporter_class=None,
                           binding_field='prestashop_bind_ids',
                           binding_extra_vals=None):
        """
        Taken from connector-magento.
        Export a dependency. The exporter class is a subclass of
        ``PrestashopExporter``. If a more precise class need to be defined,
        it can be passed to the ``exporter_class`` keyword argument.

        .. warning:: a commit is done at the end of the export of each
                     dependency. The reason for that is that we pushed a record
                     on the backend and we absolutely have to keep its ID.

                     So you *must* take care not to modify the Odoo
                     database during an export, excepted when writing
                     back the external ID or eventually to store
                     external data that we have to keep on this side.

                     You should call this method only at the beginning
                     of the exporter synchronization,
                     in :meth:`~._export_dependencies`.

        :param relation: record to export if not already exported
        :type relation: :py:class:`openerp.models.BaseModel`
        :param binding_model: name of the binding model for the relation
        :type binding_model: str | unicode
        :param exporter_cls: :py:class:`openerp.addons.connector\
                                        .connector.ConnectorUnit`
                             class or parent class to use for the export.
                             By default: PrestashopExporter
        :type exporter_cls: :py:class:`openerp.addons.connector\
                                       .connector.MetaConnectorUnit`
        :param binding_field: name of the one2many field on a normal
                              record that points to the binding record
                              (default: prestashop_bind_ids).
                              It is used only when the relation is not
                              a binding but is a normal record.
        :type binding_field: str | unicode
        :binding_extra_vals:  In case we want to create a new binding
                              pass extra values for this binding
        :type binding_extra_vals: dict
        """
        if not relation:
            return
        if exporter_class is None:
            exporter_class = PrestashopExporter
        rel_binder = self.binder_for(binding_model)
        # wrap is typically True if the relation is for instance a
        # 'product.template' record but the binding model is
        # 'prestashop.product.template'
        wrap = relation._model._name != binding_model

        if wrap and hasattr(relation, binding_field):
            domain = [('openerp_id', '=', relation.id),
                      ('backend_id', '=', self.backend_record.id)]
            binding = self.env[binding_model].search(domain)
            if binding:
                assert len(binding) == 1, (
                    'only 1 binding for a backend is '
                    'supported in _export_dependency')
            # we are working with a unwrapped record (e.g.
            # product.category) and the binding does not exist yet.
            # Example: I created a product.template and its binding
            # prestashop.product.template and we are exporting it, but we need
            # to create the binding for the product.category on which it
            # depends.
            else:
                bind_values = {'backend_id': self.backend_record.id,
                               'openerp_id': relation.id}
                if binding_extra_vals:
                    bind_values.update(binding_extra_vals)
                # If 2 jobs create it at the same time, retry
                # one later. A unique constraint (backend_id,
                # openerp_id) should exist on the binding model
                with self._retry_unique_violation():
                    binding = (self.env[binding_model]
                               .with_context(connector_no_export=True)
                               .sudo()
                               .create(bind_values))
                    # Eager commit to avoid having 2 jobs
                    # exporting at the same time. The constraint
                    # will pop if an other job already created
                    # the same binding. It will be caught and
                    # raise a RetryableJobError.
                    self.session.commit()
        else:
            # If magento_bind_ids does not exist we are typically in a
            # "direct" binding (the binding record is the same record).
            # If wrap is True, relation is already a binding record.
            binding = relation

        if not rel_binder.to_backend(binding):
            exporter = self.unit_for(exporter_class, model=binding_model)
            exporter.run(binding.id)

    def _export_dependencies(self):
        """ Export the dependencies for the record"""
        return

    def _map_data(self, fields=None):
        """ Convert the external record to OpenERP """
        #mappper.convert is deprecate using mapper.map_record
        #self.mapper.convert(self.erp_record, fields=fields)
        map_record = self.mapper.map_record(self.erp_record)
        #record = map_record.values(for_create=True)
        #self._validate_data(record)

    def _validate_data(self, data):
        """ Check if the values to import are correct

        Pro-actively check before the ``Model.create`` or
        ``Model.update`` if some fields are missing

        Raise `InvalidDataError`
        """
        return

    def _after_export(self):
        """Create records of dependants prestashop objects"""
        return

    def _create(self, data):
        """ Create the Prestashop record """
        return self.backend_adapter.create(data)

    def _update(self, data):
        """ Update an Prestashop record """
        assert self.prestashop_id
        self.backend_adapter.write(self.prestashop_id, data)

    def _run(self, fields=None):
        """ Flow of the synchronization, implemented in inherited classes"""
        assert self.binding_id
        assert self.erp_record

        if not self.prestashop_id:
            fields = None  # should be created with all the fields

        if self._has_to_skip():
            return

        # export the missing linked resources
        self._export_dependencies()
        map_record = self.mapper.map_record(self.erp_record)
        #a = self.get_record_by_lang()
        #map_record = self.mapper.map_record(fields)
        #a = self.mapper.convert_languages(self.mapper.translatable_fields)
        #self._map_data(fields=fields)

        if self.prestashop_id:
            #record = self.mapper.data
            record = map_record.values()
            if not record:
                return _('Nothing to export.')
            # special check on data before export
            self._validate_data(record)
            self._update(record)
        else:
            #record = self.mapper.data_for_create
            record = map_record.values(for_create=True)
            if not record:
                return _('Nothing to export.')
            # special check on data before export
            self._validate_data(record)
            self.prestashop_id = self._create(record)
            self._after_export()
        message = _('Record exported with ID %s on Prestashop.')
        return message % self.prestashop_id


class TranslationPrestashopExporter(PrestashopExporter):

    @property
    def mapper(self):
        if self._mapper is None:
            self._mapper = self.unit_for(TranslationPrestashopExportMapper)
        return self._mapper

    def _map_data(self, fields=None):
        """ Convert the external record to OpenERP """
        self.mapper.convert(self.get_record_by_lang(), fields=fields)

    def get_record_by_lang(self, record_id):
        # get the backend's languages
        languages = self.backend_record.language_ids
        records = {}
        # for each languages:
        for language in languages:
            # get the translated record
            record = self.model.with_context(lang=language['code']).browse(
                record_id
            )
            # put it in the dict
            records[language['prestashop_id']] = record
        return records


@job
@related_action(action=unwrap_binding)
def export_record(session, model_name, binding_id, fields=None):
    """ Export a record on Prestashop """
    record = session.env[model_name].browse(binding_id)
    env = get_environment(session, model_name, record.backend_id.id)
    exporter = env.get_connector_unit(PrestashopExporter)
    #TODO FIX PRESTASHOP
    #prestashop do not support partial edit
    fields = None

    return exporter.run(binding_id, fields=fields)
