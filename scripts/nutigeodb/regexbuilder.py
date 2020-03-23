import re

class RegexBuilder(object):
  def __init__(self):
    self.charSetsMap = {}

  def add(self, sample):
    if isinstance(sample, unicode):
      sample = sample.encode('utf8')
    charSets = self.charSetsMap.get(len(sample), [set() for c in sample])
    for i, c in enumerate(sample):
      charSets[i].add(c)
    self.charSetsMap[len(sample)] = charSets

  def build(self):
    regexes = []
    for n, charSets in self.charSetsMap.items():
      regex = ""
      for charSet in charSets:
        chars = []
        for c in sorted(list(charSet)):
          if c.isalnum() or c in [' ', '-']:
            chars.append(c.lower())
          else:
            chars = None
            break
        regex += ("[" + "".join(chars) + "]" if chars is not None else ".")
      regexes.append(regex)
    return "|".join(regexes)
