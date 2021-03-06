## -*- coding: utf-8 -*-
###############################################################################
#
#   PrestashopERPconnect for OpenERP
#   Copyright (C) 2013 Akretion (http://www.akretion.com).
#   Copyright (C) 2013 Camptocamp (http://www.camptocamp.com)
#   @author Sébastien BEAU <sebastien.beau@akretion.com>
#   @author Guewen Baconnier <guewen.baconnier@camptocamp.com>
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

import openerp
from datetime import datetime
from openerp.tools import DEFAULT_SERVER_DATETIME_FORMAT
from openerp.addons.connector.connector import Binder
from ..backend import prestashop


class PrestashopBinder(Binder):
    """ Generic Binder for Prestshop """


@prestashop
class PrestashopModelBinder(PrestashopBinder):
    """
    Bindings are done directly on the model
    """
    _model_name = [
        'prestashop.shop.group',
        'prestashop.shop',
        'prestashop.res.partner',
        'prestashop.address',
        'prestashop.res.partner.category',
        'prestashop.res.lang',
        'prestashop.res.country',
        'prestashop.res.currency',
        'prestashop.account.tax',
        'prestashop.account.tax.group',
        'prestashop.product.category',
        'prestashop.product.image',
#        'prestashop.product.product',
        'prestashop.product.template',
        'prestashop.product.combination',
        'prestashop.product.combination.option',
        'prestashop.product.combination.option.value',
        'prestashop.sale.order',
        'prestashop.sale.order.state',
        'prestashop.delivery.carrier',
        'prestashop.refund',
        'prestashop.supplier',
        'prestashop.product.supplierinfo',
        'prestashop.mail.message',
        'prestashop.groups.pricelist',
    ]

    def to_openerp(self, external_id, unwrap=False, browse=False):
        """ Give the OpenERP ID for an external ID

        :param external_id: external ID for which we want the OpenERP ID
        :param unwrap: if True, returns the normal record (the one
                       inherits'ed), else return the binding record
        :param browse: if True, returns a recordset
        :return: a recordset of one record, depending on the value of unwrap,
                 or an empty recordset if no binding is found
        :rtype: recordset
        """
        bindings = self.model.with_context(active_test=False).search(
            [('prestashop_id', '=', str(external_id)),
             ('backend_id', '=', self.backend_record.id)]
            )
        if not bindings:
            return self.model.browse() if browse else None
        assert len(bindings) == 1, "Several records found: %s" % (bindings,)
        if unwrap:
            return bindings.openerp_id if browse else bindings.openerp_id.id
        else:
            return bindings if browse else bindings.id

    def to_backend(self, record_id, wrap=False):
        """ Give the external ID for an OpenERP ID

        :param record_id: OpenERP ID for which we want the external id
                          or a recordset with one record
        :param wrap: if False, record_id is the ID of the binding,
            if True, record_id is the ID of the normal record, the
            method will search the corresponding binding and returns
            the backend id of the binding
        :return: backend identifier of the record
        """
        record = self.model.browse()
        if isinstance(record_id, openerp.models.BaseModel):
            record_id.ensure_one()
            record = record_id
            record_id = record_id.id
        if wrap:
            binding = self.model.with_context(active_test=False).search(
                [('openerp_id', '=', record_id),
                 ('backend_id', '=', self.backend_record.id),
                 ]
            )
            if binding:
                binding.ensure_one()
                return binding.prestashop_id
            else:
                return None
        if not record:
            record = self.model.browse(record_id)
        assert record
        return record.prestashop_id

    def bind(self, external_id, openerp_id):
        """ Create the link between an external ID and an OpenERP ID

        :param external_id: External ID to bind
        :param openerp_id: OpenERP ID to bind
        :type openerp_id: int
        """
        # avoid to trigger the export when we modify the `prestashop_id`
        now_fmt = datetime.now().strftime(DEFAULT_SERVER_DATETIME_FORMAT)
        if not isinstance(openerp_id, openerp.models.BaseModel):
            openerp_id = self.model.browse(openerp_id)
        openerp_id.with_context(connector_no_export=True).write(
            {'prestashop_id': str(external_id),
             'sync_date': now_fmt})

    def unwrap_binding(self, binding_id, browse=False):
        """ For a binding record, gives the normal record.

        Example: when called with a ``prestashop.product.product`` id,
        it will return the corresponding ``product.product`` id.

        :param browse: when True, returns a browse_record instance
                       rather than an ID
        """
        if isinstance(binding_id, openerp.models.BaseModel):
            binding = binding_id
        else:
            binding = self.model.browse(binding_id)

        openerp_record = binding.openerp_id
        if browse:
            return openerp_record
        return openerp_record.id

    def unwrap_model(self):
        """ For a binding model, gives the name of the normal model.

        Example: when called on a binder for ``prestashop.product.product``,
        it will return ``product.product``.

        This binder assumes that the normal model lays in ``openerp_id`` since
        this is the field we use in the ``_inherits`` bindings.
        """
        try:
            column = self.model._fields['openerp_id']
        except KeyError:
            raise ValueError('Cannot unwrap model %s, because it has '
                             'no openerp_id field' % self.model._name)
        return column.comodel_name
