document$.subscribe(function() {
  setTimeout(function() {
    var tables = document.querySelectorAll('article table:not(.no-sort)');
    tables.forEach(function(table) {
      new Tablesort(table);
    });
  }, 200);
});
