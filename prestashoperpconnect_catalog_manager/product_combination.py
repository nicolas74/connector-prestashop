# -*- encoding: utf-8 -*-
# #############################################################################
#
#   Prestashop_catalog_manager for OpenERP
#   Copyright (C) 2012-TODAY Akretion <http://www.akretion.com>.
#   All Rights Reserved
#   @author : Sébastien BEAU <sebastien.beau@akretion.com>
#             Benoît GUILLOT <benoit.guillot@akretion.com>
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU Affero General Public License as
#   published by the Free Software Foundation, either version 3 of the
#   License, or (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU Affero General Public License for more details.
#
#   You should have received a copy of the GNU Affero General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from openerp.addons.connector.queue.job import job
from openerp.addons.connector.event import on_record_create, on_record_write
from openerp.addons.connector.unit.mapper import (
    ExportMapper,
    mapping,
    changed_by
)

from openerp.addons.prestashoperpconnect.unit.export_synchronizer import (
    TranslationPrestashopExporter,
    export_record
)

from openerp.addons.prestashoperpconnect.unit.mapper import \
    TranslationPrestashopExportMapper
from openerp.addons.prestashoperpconnect.connector import get_environment
from openerp.addons.prestashoperpconnect.backend import prestashop
from openerp.addons.prestashoperpconnect.product import INVENTORY_FIELDS
from openerp.osv import fields, orm
import openerp.addons.decimal_precision as dp
import openerp.addons.prestashoperpconnect.consumer as prestashoperpconnect


@on_record_create(model_names='prestashop.product.combination')
def prestashop_product_combination_create(session, model_name, record_id, vals):
    if session.context.get('connector_no_export'):
        return
    prestashoperpconnect.delay_export(session, model_name, record_id, vals)


@on_record_write(model_names='prestashop.product.combination')
def prestashop_product_combination_write(session, model_name, record_id, vals):
    if session.context.get('connector_no_export'):
        return
    fields = list(set(vals).difference(set(INVENTORY_FIELDS)))
    if fields:
        prestashoperpconnect.delay_export(
            session, model_name, record_id, fields=vals)


@on_record_write(model_names='product.product')
def product_product_write(session, model_name, record_id, vals):
    if session.context.get('connector_no_export'):
        return
    record = session.env[model_name].browse(record_id)
    if not record.is_product_variant:
        return
    prestashoperpconnect.delay_export_all_bindings(
        session, model_name, record_id, vals)



#@on_record_create(model_names='product.product')
#def product_product_create(session, model_name, record_id, fields=None):
#    attribute_ext_id = session.pool[
#        'prestashop.product.combination'].create(
#        session.cr, session.uid, {
#            'backend_id': self.backend_record.id,
#            'openerp_id': value.attribute_id.id}, context=ctx)
#def product_product_create(session, model_name, record_id, fields):
#    if session.context.get('connector_no_export'):
#        return
#    model = session.pool.get(model_name)
#    record = model.browse(session.cr, session.uid,
#                          record_id, context=session.context)
#    #if not record.is_product_variant:
#    #    return
#    for binding in record.prestashop_bind_ids:
#        export_record.delay(session,
#                            'prestashop.product.combination', binding.id,
#                            fields, priority=20)


class prestashop_product_combination(orm.Model):
    _inherit = 'prestashop.product.combination'
    _columns = {
        'minimal_quantity': fields.integer(
            'Minimal Quantity',
            help='Minimal Sale quantity',
            )}
    _defaults = {
        'minimal_quantity': 1,
    }


@prestashop
class ProductCombinationExport(TranslationPrestashopExporter):
    _model_name = 'prestashop.product.combination'

    def _export_dependencies(self):
        """ Export the dependencies for the product"""
        #TODO add export of category
        attribute_binder = self.binder_for(
            'prestashop.product.combination.option')
        option_binder = self.binder_for(
            'prestashop.product.combination.option.value')
        for value in self.erp_record.attribute_value_ids:
            attribute_ext_id = attribute_binder.to_backend(
                value.attribute_id.id, wrap=True)
            if not attribute_ext_id:
                ctx = self.session.context.copy()
                ctx['connector_no_export'] = True
                attribute_ext_id = self.session.pool[
                    'prestashop.product.combination.option'].create(
                    self.session.cr, self.session.uid, {
                        'backend_id': self.backend_record.id,
                        'openerp_id': value.attribute_id.id}, context=ctx)
                export_record(
                    self.session,
                    'prestashop.product.combination.option',
                    attribute_ext_id
                )
            value_ext_id = option_binder.to_backend(value.id, wrap=True)
            if not value_ext_id:
                ctx = self.session.context.copy()
                ctx['connector_no_export'] = True
                value_ext_id = self.session.pool[
                    'prestashop.product.combination.option.value'].create(
                    self.session.cr, self.session.uid, {
                        'backend_id': self.backend_record.id,
                        'openerp_id': value.val_id.id,
                        'id_attribute_group': attribute_ext_id}, context=ctx)
                export_record(self.session,
                              'prestashop.product.combination.option.value',
                              value_ext_id)


@prestashop
class ProductCombinationExportMapper(TranslationPrestashopExportMapper):
    _model_name = 'prestashop.product.combination'

    direct = [
        ('default_code', 'reference'),
        ('active', 'active'),#TODO agregar el campo de default caracteristica
        ('ean13', 'ean13'),
        ('default_on', 'default_on'),
        ('minimal_quantity', 'minimal_quantity')
    ]

    def get_main_template_id(self, record):
        template_binder = self.binder_for(
            'prestashop.product.template')
        return template_binder.to_backend(record.main_template_id.id)

    @changed_by('main_template_id')
    @mapping
    def main_template_id(self, record):
        return {'id_product': self.get_main_template_id(record)}

    def _get_product_option_value(self, record):
        option_value = []
        option_binder = self.binder_for(
            'prestashop.product.combination.option.value')
        for value in record.attribute_value_ids:
            value_ext_id = option_binder.to_backend(value.id, wrap=True)
            if value_ext_id:
                option_value.append({'id': value_ext_id})
        return option_value

    @changed_by('attribute_value_ids')
    @mapping
    def associations(self, record):
        return {
            'associations': {
                'product_option_values': {
                    'product_option_value':
                    self._get_product_option_value(record)},
            }
        }
