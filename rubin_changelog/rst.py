#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import logging
from typing import List

from rstcloth import RstCloth

from rstcloth.rstcloth import Table
from sortedcontainers import SortedDict, SortedList
from .tag import Tag
from dateutil.parser import parse

log = logging.getLogger(__name__)


class Writer:
    """Create RST output files"""

    def __init__(self, outputdir: str):
        self._outputdir = outputdir

    def write_products(self, products: SortedList, ncols: int = 5) -> None:
        """write a list of all eups products into a table

        Parameters
        ----------
        products: `SortedList[str]`
            sorted list of product names
        ncols: `int`
             (Default value = 5)
             number of table rows

        Returns
        -------

        """
        products_len = len(products)
        url = 'https://github.com/lsst/'
        cols = list()
        r = products_len // ncols + (products_len % ncols) // ncols
        for i in range(r):
            rows = list()
            for j in range(ncols):
                index = i * ncols + j
                if index < products_len:
                    product = products[index]
                    link = f'`{product} <{url}{product}>`_'
                    rows.append(link)
                else:
                    rows.append('')
            cols.append(rows)
        doc = RstCloth(line_width=80)
        doc.h2('Products')
        doc.newline()
        doc.table(['Products', '', '', '', ''], data=cols)
        doc.write(self._outputdir + '/products.rst')

    # this is a workaround for the broken indent in rstcloth tables
    @staticmethod
    def _write_table(doc: RstCloth, header: List, data: List, indent=0) -> None:
        """helper function to write RstCloth tables with indentation

        Parameters
        ----------
        doc : `RstCloth`
            RST document to write to
        header : `List[str]`
            list of table headers
        data : `List[list[str]]`
            row/col data of table
        indent : `int`
             (Default value = 0)
             indent level

        Returns
        -------

        """
        doc.directive('table', indent=indent)
        doc.field('class', 'datatable', indent + 3)
        doc.newline()
        table = Table(header, data)
        for line in table.render().split('\n'):
            doc.content(line, indent + 3, False)

    def write_releases(self, jira: dict, tags: SortedDict, eups_diff: SortedDict) -> None:
        """write RST file with a table of release information

        Parameters
        ----------
        jira: `Dict`
            dictionary with JIRA tickets
        tags: `SortedDict`
            sorted dictionary with tqg data
        eups_diff: `SortedDict` :
            sorted dictionary with added/removed eups packages

        Returns
        -------

        """
        index = SortedList()
        weekly_flag = False
        summary = RstCloth(line_width=80)
        summary.h2("Summary")
        summary.newline()
        for tag in tags:
            rtag = Tag(tag)
            if rtag in eups_diff \
                    or rtag.rel_name() == 'main' \
                    or rtag.is_weekly():
                index.add(rtag)
        for rtag in reversed(index):
            tag_name = rtag.name()
            if tags[tag_name]['date'] is None:
                continue
            date = parse(tags[tag_name]['date']).strftime("%Y-%m-%d %H:%M")
            doc = RstCloth(line_width=80)
            name = rtag.rel_name()
            weekly_flag = rtag.is_weekly()
            # always add weekly tag and main
            pkg_table = list()
            doc.h2(name)
            doc.newline()
            doc.content("Released at %s" % date)
            doc.newline()
            summary.h3(name)
            summary.newline()
            summary.content("Released at %s" % date)
            summary.newline()
            if rtag in eups_diff:
                removed_pkg = eups_diff[rtag]["removed"]
                added_pkg = eups_diff[rtag]["added"]
                removed_len = len(removed_pkg)
                added_len = len(added_pkg)
                max_len = max(removed_len, added_len)
                for i in range(max_len):
                    entry1 = ''
                    entry2 = ''
                    if i < added_len:
                        entry1 = added_pkg[i]
                    if i < removed_len:
                        entry2 = removed_pkg[i]
                    pkg_table.append([entry1, entry2])
                if len(pkg_table) > 0:
                    doc.table(["Added Product(s)", "Removed Products(s)"], data=pkg_table)
                    summary.table(["Added Product(s)", "Removed Products(s)"], data=pkg_table)
                else:
                    doc.content("No products added/removed in this tag")
                    summary.content("No products added/removed in this tag")
                doc.newline()
                summary.newline()
            log.info("Writing %s" % name)
            tickets = tags[tag_name]['tickets']
            ticket_dict = SortedDict()
            row = list()
            for ticket in tickets:
                number = ticket['ticket']
                date = ticket['date'][:-4] + 'Z'
                key = 'DM-' + str(number)
                branch = ticket['branch']
                if key not in jira:
                    continue
                msg = jira[key]
                product = ticket['product']
                if number not in ticket_dict:
                    ticket_dict[number] = (msg, date, branch, [product])
                else:
                    ticket_dict[number][3].append(product)
            for number in ticket_dict:
                entry = ticket_dict[number]
                link = f"`DM-{number} <https://jira.lsstcorp.org/browse/DM-{number}>`_"
                row.append([link, entry[0], entry[1], entry[2], ', '.join(entry[3])])
            if len(row) > 0:
                self._write_table(doc, ["Ticket", "Description", 'Last Merge', "Branch", "Product"], row)
                doc.newline()
                self._write_table(summary, ["Ticket", "Description", 'Last Merge', "Branch", "Product"], row)
                summary.newline()
            else:
                doc.content("No changes in this tag")
                doc.newline()
                summary.content("No changes in this tag")
                summary.newline()
            doc.write(self._outputdir + '/' + name + '.rst')
        summary.write(self._outputdir + '/summary.rst')
        title = 'Releases'
        if weekly_flag:
            title = "Weekly Releases"
        doc = RstCloth()
        doc.h2(title)
        doc.newline()
        doc.directive('toctree')
        doc.field("caption", title, 3)
        doc.field("maxdepth", '1', 3)
        doc.field('hidden', '', 3)
        doc.newline()
        doc.content('summary', 3)
        doc.content('products', 3)
        for i in reversed(index):
            doc.content(i.rel_name(), 3)
        doc.newline()
        doc.content('- :doc:`summary`')
        doc.content('- :doc:`products`')
        for i in reversed(index):
            rel = i.rel_name()
            doc.content(f'- :doc:`{rel}`')
        doc.write(self._outputdir + '/' + '/index.rst')
