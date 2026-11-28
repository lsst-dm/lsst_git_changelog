// Natural sort algorithm for Tablesort
Tablesort.extend('natural', function(item) {
  return /^[A-Za-z]+-\d+$/.test(item); // Pattern for DM-123, SP-456 etc.
}, function(a, b) {
  // Extract prefix and number parts
  const extract = (str) => {
    const match = str.match(/^([A-Za-z]+)-(\d+)$/);
    return match ? [match[1].toLowerCase(), parseInt(match[2], 10)] : [str.toLowerCase(), 0];
  };
  
  const [aPrefix, aNum] = extract(a);
  const [bPrefix, bNum] = extract(b);
  
  // First sort by prefix, then by number
  return aPrefix.localeCompare(bPrefix) || (aNum - bNum);
});

document$.subscribe(function() {
  setTimeout(function() {
    document.querySelectorAll('article table').forEach(table => {
      // Apply natural sort to first column (change 0 to your column index)
      new Tablesort(table, { 
        descending: false,
        column: 0,
        columnTypes: ['natural']
      });
    });
  }, 200);
});
