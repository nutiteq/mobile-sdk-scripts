for row in file('iso_languages.tsv', 'rb'):
  cols = row.split('\t')
  if len(cols) < 4:
    continue
  iso3 = cols[0]
  iso2 = cols[3]
  if iso2:
    print ('"%s": "%s",' % (iso3, iso2)),
