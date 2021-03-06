# -*- coding: utf-8 -*-
##############################################################################
#
#    Prestashoperpconnect : OpenERP-PrestaShop connector
#    Copyright (C) 2013 Akretion (http://www.akretion.com/)
#    Copyright 2013 Camptocamp SA
#    @author: Alexis de Lattre <alexis.delattre@akretion.com>
#    @author Sébastien BEAU <sebastien.beau@akretion.com>
#    @author: Guewen Baconnier
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
from decimal import Decimal

from openerp.tools.translate import _
from openerp.addons.connector.unit.mapper import (
    mapping,
    ImportMapper,
    ExportMapper
)
from ..backend import prestashop
from ..connector import add_checkpoint
from backend_adapter import GenericAdapter
from backend_adapter import PrestaShopCRUDAdapter
from openerp.addons.connector_ecommerce.unit.sale_order_onchange import (
    SaleOrderOnChange)
from openerp.addons.connector.connector import Binder
from openerp.addons.connector.unit.mapper import only_create
from openerp.addons.connector_ecommerce.sale import ShippingLineBuilder

import logging
_logger = logging.getLogger(__name__)

class PrestashopImportMapper(ImportMapper):

    #get_openerp_id is deprecated use the binder intead
    #we should have only 1 way to map the field to avoid error
    def get_openerp_id(self, model, prestashop_id):
        '''
        Returns an openerp id from a model name and a prestashop_id.

        This permits to find the openerp id through the external application
        model in Erp.
        '''
        binder = self.binder_for(model)
        erp_ps_id = binder.to_openerp(prestashop_id)
        if erp_ps_id is None:
            return None

        model = self.session.pool.get(model)
        erp_ps_object = model.read(
            self.session.cr,
            self.session.uid,
            erp_ps_id,
            ['openerp_id'],
            context=self.session.context
        )
        return erp_ps_object['openerp_id'][0]


@prestashop
class ShopGroupImportMapper(PrestashopImportMapper):
    _model_name = 'prestashop.shop.group'

    direct = [('name', 'name')]

    @mapping
    def name(self, record):
        name = record['name']
        if name is None:
            name = _('Undefined')
        return {'name': name}

    @mapping
    def backend_id(self, record):
        return {'backend_id': self.backend_record.id}


@prestashop
class ShopImportMapper(PrestashopImportMapper):
    _model_name = 'prestashop.shop'

    direct = [
        ('name', 'name'),
    ]

    @mapping
    def shop_group_id(self, record):
        binder = self.binder_for(model='prestashop.shop.group')
        binding_id = binder.to_openerp(record['id_shop_group'])
        return {'shop_group_id': binding_id}

    @mapping
    def backend_id(self, record):
        return {'backend_id': self.backend_record.id}

    @mapping
    def company_id(self, record):
        return {'company_id': self.backend_record.company_id.id}

    @mapping
    def warehouse_id(self, record):
        return {'warehouse_id': self.backend_record.warehouse_id.id}
    @mapping
    def opener_id(self, record):
        return {'openerp_id': self.backend_record.warehouse_id.id}



@prestashop
class PartnerCategoryImportMapper(PrestashopImportMapper):
    _model_name = 'prestashop.res.partner.category'

    direct = [
        ('name', 'name'),
        ('date_add', 'date_add'),
        ('date_upd', 'date_upd'),
    ]

    @mapping
    def prestashop_id(self, record):
        return {'prestashop_id': record['id']}

    @mapping
    def backend_id(self, record):
        return {'backend_id': self.backend_record.id}


@prestashop
class PartnerImportMapper(PrestashopImportMapper):
    _model_name = 'prestashop.res.partner'

    direct = [
        ('date_add', 'date_add'),
        ('date_upd', 'date_upd'),
        ('email', 'email'),
        ('newsletter', 'newsletter'),
        ('company', 'company'),
        ('active', 'active'),
        ('note', 'comment'),
        ('id_shop_group', 'shop_group_id'),
        ('id_shop', 'shop_id'),
        ('id_default_group', 'default_category_id'),
    ]

    @mapping
    def pricelist(self, record):
        binder = self.unit_for(Binder, 'prestashop.groups.pricelist')
        pricelist_id = binder.to_openerp(record['id_default_group'], unwrap=True)
        if not pricelist_id:
            return {}
        return {'property_product_pricelist': pricelist_id}

    @mapping
    def birthday(self, record):
        if record['birthday'] in ['0000-00-00', '']:
            return {}
        return {'birthday': record['birthday']}

    @mapping
    def name(self, record):
        name = ""
        if record['firstname']:
            name += record['firstname']
        if record['lastname']:
            if len(name) != 0:
                name += " "
            name += record['lastname']
        return {'name': name}

    @mapping
    def groups(self, record):
        groups = record.get('associations', {}).get('groups', {}).get('group', [])
        if not isinstance(groups, list):
            groups = [groups]
        partner_categories = []
        for group in groups:
            binder = self.binder_for(
                'prestashop.res.partner.category'
            )
            category_id = binder.to_openerp(group['id'])
            partner_categories.append(category_id)

        return {'group_ids': [(6, 0, partner_categories)]}

    @mapping
    def backend_id(self, record):
        return {'backend_id': self.backend_record.id}

    @mapping
    def lang(self, record):
        binder = self.binder_for('prestashop.res.lang')
        erp_lang_id = None
        if record.get('id_lang'):
            erp_lang_id = binder.to_openerp(record['id_lang'])
        if erp_lang_id is None:
            data_obj = self.session.env['ir.model.data']
            erp_lang_id = data_obj.get_object_reference(
                'base',
                'lang_en')[1]
        model = self.session.env['prestashop.res.lang']
        erp_lang = model.browse(
            erp_lang_id
        )
        return {'lang': erp_lang.code}

    @mapping
    def customer(self, record):
        return {'customer': True}

    @mapping
    def is_company(self, record):
        # This is sad because we _have_ to have a company partner if we want to
        # store multiple adresses... but... well... we have customers who want
        # to be billed at home and be delivered at work... (...)...
        return {'is_company': True}

    @mapping
    def company_id(self, record):
        return {'company_id': self.backend_record.company_id.id}


@prestashop
class SupplierMapper(PrestashopImportMapper):
    _model_name = 'prestashop.supplier'

    direct = [
        ('name', 'name'),
        ('id', 'prestashop_id'),
        ('active', 'active'),
    ]

    @mapping
    def company_id(self, record):
        return {'company_id': self.backend_record.company_id.id}

    @mapping
    def backend_id(self, record):
        return {'backend_id': self.backend_record.id}

    @mapping
    def supplier(self, record):
        return {
            'supplier': True,
            'is_company': True,
            'customer': False,
        }

    @mapping
    def image(self, record):
        supplier_image_adapter = self.unit_for(
            PrestaShopCRUDAdapter, 'prestashop.supplier.image'
        )
        try:
            return {'image': supplier_image_adapter.read(record['id'])}
        except:
            return {}


@prestashop
class AddressImportMapper(PrestashopImportMapper):
    _model_name = 'prestashop.address'

    direct = [
        ('address1', 'street'),
        ('address2', 'street2'),
        ('city', 'city'),
        ('other', 'comment'),
        ('phone', 'phone'),
        ('phone_mobile', 'mobile'),
        ('postcode', 'zip'),
        ('date_add', 'date_add'),
        ('date_upd', 'date_upd'),
        ('id_customer', 'prestashop_partner_id'),
    ]

    @mapping
    def parent_id(self, record):
        binder = self.binder_for('prestashop.res.partner')
        parent_id = binder.to_openerp(record['id_customer'], unwrap=True)
        if record['vat_number']:
            vat_number = record['vat_number'].replace('.', '').replace(' ', '')
            if self._check_vat(vat_number):
                self.session.write(
                    'res.partner',
                    [parent_id],
                    {'vat': vat_number}
                )
            else:
                add_checkpoint(
                    self.session,
                    'res.partner',
                    parent_id,
                    self.backend_record.id
                )
        return {'parent_id': parent_id}

    def _check_vat(self, vat):
        vat_country, vat_number = vat[:2].lower(), vat[2:]
        return self.session.pool['res.partner'].simple_vat_check(
            self.session.cr,
            self.session.uid,
            vat_country,
            vat_number,
            context=self.session.context
        )

    @mapping
    def name(self, record):
        name = ""
        if record['firstname']:
            name += record['firstname']
        if record['lastname']:
            if name:
                name += " "
            name += record['lastname']
        if record['alias']:
            if name:
                name += " "
            name += '('+record['alias']+')'
        return {'name': name}

    @mapping
    def customer(self, record):
        return {'customer': True}

    @mapping
    def country(self, record):
        if record.get('id_country'):
            binder = self.binder_for('prestashop.res.country')
            erp_country_id = binder.to_openerp(record['id_country'], unwrap=True)
            return {'country_id': erp_country_id}
        return {}

    @mapping
    def company_id(self, record):
        return {'company_id': self.backend_record.company_id.id}

@prestashop
class SaleOrderStateMapper(PrestashopImportMapper):
    _model_name = 'prestashop.sale.order.state'

    direct = [
        ('name', 'name'),
    ]

    @mapping
    def backend_id(self, record):
        return {'backend_id': self.backend_record.id}

    @mapping
    def company_id(self, record):
        return {'company_id': self.backend_record.company_id.id}


@prestashop
class SaleOrderMapper(PrestashopImportMapper):
    _model_name = 'prestashop.sale.order'

    direct = [
        ('date_add', 'date_order'),
        ('invoice_number','prestashop_invoice_number'),
        ('delivery_number','prestashop_delivery_number'),
        ('total_paid', 'total_amount'),
        ('total_shipping_tax_incl', 'total_shipping_tax_included'),
        ('total_shipping_tax_excl', 'total_shipping_tax_excluded')
    ]

    def _get_sale_order_lines(self, record):
        order_rows = record['associations'].get('order_rows', {}).get('order_rows', [])
        if isinstance(order_rows, dict):
            return [order_rows]
        return order_rows

    children = [
        (
            _get_sale_order_lines,
            'prestashop_order_line_ids',
            'prestashop.sale.order.line'
        ),
    ]

    def _map_child(self, map_record, from_attr, to_attr, model_name):
        source = map_record.source
        # TODO patch ImportMapper in connector to support callable
        if callable(from_attr):
            child_records = from_attr(self, source)
        else:
            child_records = source[from_attr]

        children = []
        for child_record in child_records:
            adapter = self.unit_for(GenericAdapter, model_name)
            detail_record = adapter.read(child_record['id'])

            mapper = self._get_map_child_unit(model_name)
            items = mapper.get_items(
                [detail_record], map_record, to_attr, options=self.options
            )
            children.extend(items)

        discount_lines = self._get_discounts_lines(source)
        children.extend(discount_lines)
        return children

    def _get_discounts_lines(self, record):
        if record['total_discounts'] == '0.00':
            return []
        adapter = self.unit_for(
            GenericAdapter, 'prestashop.sale.order.line.discount')
        discount_ids = adapter.search({'filter[id_order]': record['id']})
        discount_mappers = []
        for discount_id in discount_ids:
            discount = adapter.read(discount_id)
            mapper = self.unit_for(
                ImportMapper, 'prestashop.sale.order.line.discount')
            map_record = mapper.map_record(discount, parent=record)
            map_values = map_record.values()
            discount_mappers.append((0, 0, map_values))
        return discount_mappers

    def _sale_order_exists(self, name):
        sales = self.session.env['sale.order'].search([
            ('name', '=', name),
            ('company_id', '=', self.backend_record.company_id.id),
        ])
        return len(sales) == 1

    @mapping
    def name(self, record):
        basename = record['reference']
        if not self._sale_order_exists(basename):
            return {"name": basename}
        i = 1
        name = basename + '_%d' % (i)
        while self._sale_order_exists(name):
            i += 1
            name = basename + '_%d' % (i)
        return {"name": name}

    @mapping
    def shipping(self, record):
        shipping_tax_incl = float(record['total_shipping_tax_incl'])
        shipping_tax_excl = float(record['total_shipping_tax_excl'])
        return {
            'shipping_amount_tax_included': shipping_tax_incl,
            'shipping_amount_tax_excluded': shipping_tax_excl,
        }

    @mapping
    def shop_id(self, record):
        if record['id_shop'] == '0':
            shop_ids = self.session.search('prestashop.shop', [
                ('backend_id', '=', self.backend_record.id)
            ])
            return {'shop_id': shop_ids[0]}
        shop_binder = self.binder_for('prestashop.shop')
        shop_id = shop_binder.to_openerp(record['id_shop'])
        return {'shop_id': shop_id}

    @mapping
    def partner_id(self, record):
        binder = self.binder_for('prestashop.res.partner')
        return {'partner_id': binder.to_openerp(
            record['id_customer'], unwrap=True
        )}

    @mapping
    def partner_invoice_id(self, record):
        binder = self.binder_for('prestashop.address')
        return {'partner_invoice_id': binder.to_openerp(
            record['id_address_invoice'], unwrap=True
        )}

    @mapping
    def partner_shipping_id(self, record):
        binder = self.binder_for('prestashop.address')
        return {'partner_shipping_id': binder.to_openerp(
            record['id_address_delivery'], unwrap=True
        )}

#    @mapping
  #  def pricelist_id(self, record):
 #       return {'pricelist_id': 1}

    @mapping
    def backend_id(self, record):
        return {'backend_id': self.backend_record.id}

    @mapping
    def payment(self, record):
        methods = self.session.env['payment.method'].search(
            [
                ('name', '=', record['payment']),
                ('company_id', '=', self.backend_record.company_id.id),
            ]
        )
        assert methods, ("Payment method '%s' has not been found ; "
                            "you should create it manually (in Sales->"
                            "Configuration->Sales->Payment Methods" %
                            record['payment'])
        method_id = methods[0].id
        return {'payment_method_id': method_id}

    @mapping
    def carrier_id(self, record):
        if record['id_carrier'] == '0':
            return {}
        binder = self.binder_for('prestashop.delivery.carrier')
        return {'carrier_id': binder.to_openerp(
            record['id_carrier'], unwrap=True
        )}

    @mapping
    def total_tax_amount(self, record):
        tax = float(record['total_paid_tax_incl'])\
                - float(record['total_paid_tax_excl'])
        return {'total_amount_tax': tax}

    @mapping
    def user_id(self, record):
        """ Do not assign to a Salesperson otherwise sales orders are hidden
        for the salespersons (access rules)"""
        return {'user_id': False}

    def _add_shipping_line(self, map_record, values):
        record = map_record.source
        amount_incl = float(record.get('total_shipping_tax_incl') or 0.0)
        amount_excl = float(record.get('total_shipping_tax_excl') or 0.0)
        if not (amount_incl or amount_excl):
            return values
        line_builder = self.unit_for(PrestashopShippingLineBuilder)
        if self.backend_record.taxes_included:
            line_builder.price_unit = amount_incl
        else:
            line_builder.price_unit = amount_excl
        if values.get('carrier_id'):
            carrier = self.env['delivery.carrier'].browse(values['carrier_id'])
            line_builder.product = carrier.product_id
        line_builder.is_delivery = True
        line = (0, 0, line_builder.get_line())
        values['order_line'].append(line)
        return values

    def finalize(self, map_record, values):
        values.setdefault('order_line', [])
        values = self._add_shipping_line(map_record, values)
        onchange = self.unit_for(SaleOrderOnChange)
        return onchange.play(values, values['prestashop_order_line_ids'])


@prestashop
class SaleOrderLineMapper(PrestashopImportMapper):
    _model_name = 'prestashop.sale.order.line'

    direct = [
        ('product_name', 'name'),
        ('id', 'sequence'),
        ('product_quantity', 'product_uom_qty'),
        ('reduction_percent', 'discount'),
    ]

    @mapping
    def prestashop_id(self, record):
        return {'prestashop_id': record['id']}

    def none_product(self, record):
        product_id = True
        if 'product_attribute_id' not in record:
            binder = self.binder_for('prestashop.product.template')
            template_id = binder.to_openerp(
                record['product_id'], unwrap=True)
            product_id = self.session.env['product.product'].search([
                ('product_tmpl_id', '=', template_id),
                ('company_id', '=', self.backend_record.company_id.id)])
        return not product_id

    @mapping
    def price_unit(self, record):
        #if self.backend_record.taxes_included or self.none_product(record):
        if self.backend_record.taxes_included:
            key = 'unit_price_tax_incl'
        else:
            key = 'unit_price_tax_excl'
        # if record['reduction_percent']:
        #     reduction = Decimal(record['reduction_percent'])
        #     price = Decimal(record[key])
        #     price_unit = price / ((100 - reduction) / 100)
        # else:
        #     price_unit = record[key]
        price_unit = record[key]
        return {'price_unit': price_unit}

    @mapping
    def product_id(self, record):
        if 'product_attribute_id' in record and record['product_attribute_id'] != '0':
            combination_binder = self.binder_for(
                'prestashop.product.combination')
            product_id = combination_binder.to_openerp(
                record['product_attribute_id'],
                unwrap=True
            )
        else:
            template_binder = self.binder_for('prestashop.product.template')
            template_id = template_binder.to_openerp(
                record['product_id'], unwrap=True)
            product = self.session.env['product.product'].search([
                ('product_tmpl_id', '=', template_id),
                ('company_id', '=', self.backend_record.company_id.id)])
            if not product:
                return self.tax_id(record)
            product_id = product[0].id
        return {'product_id': product_id}

    # def _find_tax(self, ps_tax_id):
    #     binder = self.binder_for('prestashop.account.tax')
    #     openerp_id = binder.to_openerp(ps_tax_id, unwrap=True)
    #     tax = self.session.read('account.tax', openerp_id, ['price_include', 'related_inc_tax_id'])
    #     if self.backend_record.taxes_included and not tax['price_include'] and tax['related_inc_tax_id']:
    #         return tax['related_inc_tax_id'][0]
    #     return openerp_id

    @mapping
    def tax_id(self, record):
        if self.backend_record.taxes_included:
            return_value = {}
        else:
            tax_excl = float(record['unit_price_tax_excl'])
            tax_incl = float(record['unit_price_tax_incl'])
            tax_percent = ( (tax_incl-tax_excl) / tax_excl )
            _logger.info('tax_percent =) ' + str(tax_percent))
            tax_ids = self.session.env['account.tax'].search([
                    ('amount', '=', tax_percent),
                    ('prestashop_tax_available', '=', 'True')
                    ])
            _logger.info('tax_ids =) ' + str(tax_ids))
            if len(tax_ids) >= 1:
                tax_id = tax_ids[0]
                _logger.info('tax_id =) ' + str(tax_id))
                if tax_id['type'] == 'percent':
                    return_value = {'tax_id': [(6, 0, [tax_id['id']])]}
            else:
                return_value = {}
        _logger.info('return_value =) ' + str(return_value))
        return return_value

    # @mapping
    # def tax_id(self, record):
    #     taxes = record.get('associations', {}).get('taxes', {}).get('tax', [])
    #     if not isinstance(taxes, list):
    #         taxes = [taxes]
    #     result = []
    #     for tax in taxes:
    #         openerp_id = self._find_tax(tax['id'])
    #         if openerp_id:
    #             result.append(openerp_id)
    #     if result:
    #         return {'tax_id': [(6, 0, result)]}
    #     return {}

    @mapping
    def backend_id(self, record):
        return {'backend_id': self.backend_record.id}


@prestashop
class SaleOrderLineDiscount(PrestashopImportMapper):
    _model_name = 'prestashop.sale.order.line.discount'

    direct = []

    @mapping
    def discount(self, record):
        return {
            'name': _('Discount %s') % (record['name']),
            'product_uom_qty': 1,
        }

    @mapping
    def price_unit(self, record):
        if self.backend_record.taxes_included:
            price_unit = record['value']
        else:
            price_unit = record['value_tax_excl']
        if price_unit[0] != '-':
            price_unit = '-' + price_unit
        return {'price_unit': price_unit}

    @mapping
    def product_id(self, record):
        if self.backend_record.discount_product_id:
            return {'product_id': self.backend_record.discount_product_id.id}
        data_obj = self.session.pool.get('ir.model.data')
        model_name, product_id = data_obj.get_object_reference(
            self.session.cr,
            self.session.uid,
            'connector_ecommerce',
            'product_product_discount'
        )
        return {'product_id': product_id}

    @mapping
    def backend_id(self, record):
        return {'backend_id': self.backend_record.id}


@prestashop
class TaxGroupMapper(PrestashopImportMapper):
    _model_name = 'prestashop.account.tax.group'

    direct = [
        ('name', 'name'),
    ]

    @mapping
    def backend_id(self, record):
        return {'backend_id': self.backend_record.id}

    @mapping
    def company_id(self, record):
        return {'company_id': self.backend_record.company_id.id}


@prestashop
class SupplierInfoMapper(PrestashopImportMapper):
    _model_name = 'prestashop.product.supplierinfo'

    direct = [
        ('product_supplier_reference', 'product_code'),
    ]

    @mapping
    def company_id(self, record):
        return {'company_id': self.backend_record.company_id.id}

    @mapping
    def backend_id(self, record):
        return {'backend_id': self.backend_record.id}

    @mapping
    def name(self, record):
        binder = self.unit_for(Binder, 'prestashop.supplier')
        partner_id = binder.to_openerp(record['id_supplier'], unwrap=True)
        return {'name': partner_id}

#    @mapping
#    def product_id(self, record):
#        if record['id_product_attribute'] != '0':
#            binder = self.unit_for(Binder, 'prestashop.product.combination')
#            return {'product_id': binder.to_openerp(record['id_product_attribute'], unwrap=True)}
#        binder = self.unit_for(Binder, 'prestashop.product.product')
#        return {'product_id': binder.to_openerp(record['id_product'], unwrap=True)}

    @mapping
    def product_tmpl_id(self, record):
        binder = self.unit_for(Binder, 'prestashop.product.template')
        erp_id = binder.to_openerp(record['id_product'], unwrap=True)
        #template = self.session.browse('product.template', erp_id)
        #product_tmpl_id = template.id
        #return {'product_tmpl_id': product_tmpl_id}
        return {'product_tmpl_id': erp_id}


    @mapping
    def required(self, record):
        return {'min_qty': 0.0, 'delay': 1}

class PrestashopExportMapper(ExportMapper):


    def get_changed_by_fields(self):
        """
        You can override this method to add a custom way to consider fields.
        """
        return set(self._changed_by_fields)

    def _map_direct(self, record, from_attr, to_attr):
        res = super(PrestashopExportMapper, self)._map_direct(record,
                                                              from_attr,
                                                              to_attr) or ''
        if not callable(from_attr):
            column = self.model._all_columns[from_attr].column
            if column._type == 'boolean':
                return res and 1 or 0
            elif column._type == 'float':
                res = str(res)
        return res


class TranslationPrestashopExportMapper(PrestashopExportMapper):

    def convert(self, records_by_language, fields=None):
        self.records_by_language = records_by_language
        first_key = records_by_language.keys()[0]
        self._convert(records_by_language[first_key], fields=fields)
        self._data.update(self.convert_languages(self.translatable_fields))

    def convert_languages(self, records_by_language, translatable_fields):
        res = {}
        for from_attr, to_attr in translatable_fields:
            value = {'language': []}
            for language_id, record in records_by_language.items():
                value['language'].append({
                    'attrs': {'id': str(language_id)},
                    'value': record[from_attr]
                })
            res[to_attr] = value
        return res


@prestashop
class MailMessageMapper(PrestashopImportMapper):
    _model_name = 'prestashop.mail.message'

    direct = [
        ('message', 'body'),
    ]

    @mapping
    def backend_id(self, record):
        return {'backend_id': self.backend_record.id}

    @mapping
    def type(self, record):
        return {'type': 'comment'}

    @mapping
    def object_ref(self, record):
        binder = self.unit_for(
            Binder, 'prestashop.sale.order'
        )
        order_id = binder.to_openerp(record['id_order'], unwrap=True)
        return {
            'model': 'sale.order',
            'res_id': order_id,
        }

    @mapping
    def author_id(self, record):
        if record['id_customer'] != '0':
            binder = self.unit_for(Binder, 'prestashop.res.partner')
            partner_id = binder.to_openerp(record['id_customer'], unwrap=True)
            return {'author_id': partner_id}
        return {}


@prestashop
class ProductPricelistMapper(PrestashopImportMapper):
    _model_name = 'prestashop.groups.pricelist'

    direct = [
        ('name', 'name'),
    ]

    @mapping
    def static(self, record):
        return {'active': True, 'type': 'sale'}

    @mapping
    def backend_id(self, record):
        return {'backend_id': self.backend_record.id}

    @mapping
    def company_id(self, record):
        return {'company_id': self.backend_record.company_id.id}

    @mapping
    @only_create
    def versions(self, record):
        item = {
            'min_quantity': 0,
            'sequence': 5,
            'base': 1,
            'price_discount': - float(record['reduction']) / 100.0,
        }
        version = {
            'name': 'Version',
            'active': True,
            'items_id': [(0, 0, item)],
        }
        return {'version_id': [(0, 0, version)]}


@prestashop
class PrestashopShippingLineBuilder(ShippingLineBuilder):
    _model_name = 'prestashop.sale.order'

    def get_line(self):
        vals = super(PrestashopShippingLineBuilder, self).get_line()
        if self.is_delivery:
            vals['is_delivery'] = True
        return vals
