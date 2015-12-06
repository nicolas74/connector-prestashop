# -*- coding: utf-8 -*-
##############################################################################
#
#    Author: Benoît Guillot
#    Copyright 2014 Akretion
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

"""
Related Actions for Prestashop:

Related actions are associated with jobs.
When called on a job, they will return an action to the client.

"""

import functools
from openerp.osv import orm
from openerp.tools.translate import _
from openerp.addons.connector import related_action
from .connector import get_environment
from .unit.backend_adapter import GenericAdapter
from .unit.binder import PrestashopBinder

unwrap_binding = functools.partial(related_action.unwrap_binding,
                                   binder_class=PrestashopBinder)

