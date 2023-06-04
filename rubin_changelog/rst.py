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
import sys
import textwrap

from sortedcontainers import SortedList

from .tag import Tag, ReleaseType

log = logging.getLogger(__name__)


class RstBase:
    def __init__(self, file):
        self.file = file

    def _print(self, output):
        print(output, end='', file=self.file)

    def _println(self, output):
        print(output, file=self.file)

    def _indent(self, indent):
        self._print(' ' * indent)

    def header(self, output, sep):
        self._println(output)
        self._println(sep * len(output))

    def write(self, output):
        self._print(output)

    def writeln(self, output):
        self._println(output)

    def nl(self, n=1):
        for i in range(n):
            self._println('')


class RstTable(RstBase):
    def __init__(self, rows, headers, indent=0, file=sys.stdout):
        super().__init__(file)
        self.rows = rows
        self.indent = indent
        self.headers = headers
        self.cols = len(headers)
        self.col_len = [0] * self.cols
        for row in rows + [headers]:
            for i in range(self.cols):
                col = row[i]
                length = 0
                if type(col) == list:
                    for c in col:
                        if len(c) > length:
                            length = len(c)
                else:
                    length = len(col)
                if length > self.col_len[i]:
                    self.col_len[i] = length

    def _write_table_header(self, line, sep):
        self._indent(self.indent)
        for i in range(self.cols):
            self._print(sep)
            self._print(line * self.col_len[i])
        self._println(sep)

    def _write_row(self, row, sep):
        multilines = 0
        for i in range(self.cols):
            line = row[i]
            if type(row[i]) == list:
                length = len(line)
            else:
                length = 1
            if length > multilines:
                multilines = length
        for m in range(multilines):
            self._indent(self.indent)
            for j in range(self.cols):
                line = row[j]
                if type(row[j]) == list and len(row[j]) > m:
                    line = row[j][m]
                elif m >= 1:
                    line = ''
                self._print(sep)
                self._print(line)
                self._print(' ' * (self.col_len[j] - len(line)))
            self._println(sep)
            if multilines > 1 and m < multilines - 1:
                self._indent(self.indent)
                for j in range(self.cols):
                    self._print(sep)
                    self._print(' ' * self.col_len[j])
                self._println(sep)

    def write_table(self):
        self._print(
            '.. table::\n'
            '   :class: datatable\n')
        self.nl()
        self._write_table_header('-', '+')
        self._write_row(self.headers, '|')
        self._write_table_header('=', '+')
        for row in self.rows:
            self._write_row(row, '|')
            self._write_table_header('-', '+')


class RstRelease:
    def __init__(self, release_type, release_data, repo_data, repo_map, products, package_diff):
        self.products = products
        self.repo_data = repo_data
        self.repo_map = repo_map
        self.release_data = release_data[0]
        self.tag_date = release_data[1]
        self.package_diff = package_diff[release_type]
        self.release_type = release_type
        self.subdir = 'weekly'
        self.caption = 'Weekly'
        if release_type == ReleaseType.REGULAR:
            self.subdir = 'releases'
            self.caption = "Releases"
        self.releases = SortedList()
        for r in self.release_data.keys():
            self.releases.add(Tag(r))

    @staticmethod
    def make_link(name, url, anon=False):
        trail = '_'
        if anon:
            trail = '__'
        return f"`{name} <{url}>`{trail}"

    @staticmethod
    def _escape(string: str):
        result = string
        for c in ['*', '`', '_']:
            result = result.replace(c, f'\\{c}')
        return result

    def make_table(self, release):
        result = list()
        for t, branches in release.items():
            if t is None:
                continue
            ticket = int(t)
            name = self.make_link(f"DM_{ticket:05d}", f"https://jira.lsstcorp.org/browse/DM-{ticket}")
            for b, c in branches.items():
                wrap = textwrap.TextWrapper(width=60)
                desc = wrap.wrap(self._escape(c[0]))
                wrap = textwrap.TextWrapper(width=60)
                pkg_names = list()
                for p in c[1]:
                    pkg_names.append(self.repo_map[p[0]])
                pkg = wrap.wrap(', '.join(pkg_names))
                merge_date = c[2]
                n = 0
                pkg_links = list()
                for i in range(len(pkg)):
                    temp = list()
                    for j in range(len(pkg[i].split(', '))):
                        link_name = self.repo_map[c[1][n][0]]
                        link = self.make_link(link_name, c[1][n][1], anon=True)
                        temp.append(link)
                        n = n + 1
                    pkg_links.append(', '.join(temp))
                result.append([name, desc, merge_date, b, pkg_links])
        return result

    def write_products(self):
        file = open(f'source/{self.subdir}/products.rst', 'w')
        rst = RstBase(file)
        rst.header("Products", '-')
        rst.nl(2)
        ncol = 4
        product_list = list()
        i = 0
        headers = [''] * ncol
        headers[0] = 'Products'
        for el in self.products[self.release_type]:
            if i % ncol == 0:
                product_list.append([''] * ncol)
            link = el
            if el in self.repo_data:
                owner = self.repo_data[el][0]
                repo = self.repo_data[el][1]
                link = self.make_link(el, f'https://github.com/{owner}/{repo}')
            else:
                log.warning("Product repository for %s not found", el)
            product_list[-1][i % ncol] = link
            i = i + 1
        table = RstTable(product_list, headers, 3, file)
        table.write_table()
        file.close()

    def write_index(self):
        file = open(f'source/{self.subdir}/index.rst', 'w')
        rst = RstBase(file)
        underline = '-' * len(self.caption)
        body = (
            f'{self.caption}\n'
            f'{underline}\n'
            '\n'
            '.. toctree::\n'
            f'   :caption: {self.caption}\n'
            '   :maxdepth: 1\n'
            '   :hidden:\n'
            '\n'
            '   summary\n'
            '   products\n')

        rst.write(body)
        for r in reversed(self.releases):
            name = r.name()
            eups_name = r.rel_name()
            if name == "~untagged":
                continue
            rst.writeln(f"   {eups_name}")
        rst.nl()
        rst.write(
            '- :doc:`summary`\n'
            '- :doc:`products`\n'
        )
        for r in reversed(self.releases):
            name = r.name()
            eups_name = r.rel_name()
            if name == "~untagged":
                continue
            rst.writeln(f"- :doc:`{eups_name}`")
        file.close()

    def write_product_table(self, r, rst):
        added = self.package_diff[r]["added"]
        removed = self.package_diff[r]['removed']
        pkg_table = list()
        l1 = len(added)
        l2 = len(removed)
        for i in range(max(l1, l2)):
            col = ["", ""]
            if i < l1:
                col[0] = added[i]
            if i < l2:
                col[1] = removed[i]
            pkg_table.append(col)
        if (len(pkg_table)) > 0:
            headers = ['Added', 'Removed']
            table = RstTable(pkg_table, headers, indent=3, file=rst)
            table.write_table()
            rst.nl()
        else:
            rst.writeln('No packages added/removed in this release')
            rst.nl()

    def write_releases(self):
        summary_file = open(f'source/{self.subdir}/summary.rst', 'w')
        summary = RstBase(summary_file)
        summary.header('Summary', '-')
        summary.nl()
        for r in reversed(self.releases):
            name = r.name()
            eups_name = r.rel_name()
            if name == '~untagged':
                continue
            log.info("Writing release %s", eups_name)
            file = open(f'source/{self.subdir}/{eups_name}.rst', 'w')
            rst = RstBase(file)
            rst.header(eups_name, '-')
            rst.nl()
            summary.header(eups_name, '-')
            summary.nl()
            tag_date = self.tag_date[name][1]
            if eups_name == 'main':
                release_text = f"Updated at {tag_date}"
            else:
                release_text = f"Released at {tag_date}"
            rst.writeln(release_text)
            rst.nl()
            summary.writeln(release_text)
            summary.nl()
            if r in self.package_diff:
                self.write_product_table(r, rst)
                self.write_product_table(r, summary)

            rel = self.make_table(self.release_data[name])
            headers = ['Ticket', 'Description', "Last Merge", "Branch", "Packages"]
            table1 = RstTable(rel, headers, indent=3, file=summary_file)
            table2 = RstTable(rel, headers, indent=3, file=file)
            table1.write_table()
            table2.write_table()
            rst.nl()
            summary.nl()
            file.close()

        summary_file.close()

    def write(self):
        self.write_products()
        self.write_index()
        self.write_releases()
