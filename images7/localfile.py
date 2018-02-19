"""Helper classes for dealing with local file operations."""


import os
import errno
import logging
import shutil
import re


class FileCopy(object):
    def __init__(
            self,
            source=None,
            destination=None,
            link=False,
            remove_source=False):

        self.source = source
        self.destination = destination
        self.link = link
        self.remove_source = remove_source

    def run(self):
        destination_folder = os.path.dirname(self.destination)
        os.makedirs(destination_folder, exist_ok=True)

        while True:
            try:
                if self.link:
                    logging.debug("Linking %s -> %s", self.source, self.destination)
                    os.link(self.source, self.destination)
                else:
                    logging.debug("Copying %s -> %s", self.source, self.destination)
                    shutil.copy2(self.source, self.destination)
                break
            except OSError as e:
                if e.errno == errno.EXDEV:
                    logging.warning("Cross-device link %s -> %s", self.source, self.destination)
                    self.link = False
                else:
                    logging.warning("OSError %i %s -> %s (%s)", e.errno, self.source, self.destination, str(e))
                    raise e

        if self.remove_source:
            logging.debug("Removing source %s", self.source)
            os.remove(self.source)


class FolderScanner(object):
    def __init__(self, basepath, extensions=None):
        self.basepath = basepath
        if extensions is None:
            self.extensions = None
        else:
            self.extensions = [e.lower() for e in extensions]
            self.extensions = [e if e.startswith('.') else ('.' + e) for e in self.extensions]
            self.extensions.extend([e.upper() for e in self.extensions])
        logging.debug('Scanning for file-extensions %s', str(self.extensions))

    def scan(self):
        for relative_path, directories, files in os.walk(self.basepath, followlinks=True):
            logging.debug('Scanning %s', relative_path)
            for f in files:
                if self.extensions is None or any(map(lambda e: f.endswith(e), self.extensions)):
                    path = os.path.relpath(os.path.join(relative_path, f), self.basepath)
                    if not path.startswith('.'):
                        yield path


mangled = re.compile(r'[^A-Za-z0-9-_]')
mangled_with_dots = re.compile(r'[^A-Za-z0-9-_.]')


def mangle(string, accept_dots=False):
    """
    Transliterates a string to ascii format. The only remaining characters in the
    result will be a-z, A-Z, 0-9, -, _

    :param str string: The input string
    :param bool accept_dots: Preserve dots in the incoming string
    :rtype: unicode
    """
    mstrs = { 
        # LATIN1 UPPERCASE
        u'Å': u'A',
        u'Ä': u'A',
        u'Ö': u'O',
        u'À': u'A',
        u'Á': u'A',
        u'Â': u'A',
        u'Ã': u'A',
        u'Ä': u'A',
        u'Å': u'A',
        u'Æ': u'AE',
        u'Ç': u'C',
        u'È': u'E',
        u'É': u'E',
        u'Ê': u'E',
        u'Ë': u'E',
        u'Ì': u'I',
        u'Í': u'I',
        u'Î': u'I',
        u'Ï': u'I',
        u'Ð': u'D',
        u'Ñ': u'N',
        u'Ò': u'O',
        u'Ó': u'O',
        u'Ô': u'O',
        u'Õ': u'O',
        u'Ö': u'O',
        u'Ø': u'O',
        u'Ù': u'U',
        u'Ú': u'U',
        u'Û': u'U',
        u'Ü': u'U',
        u'Ý': u'Y',
        u'Ÿ': u'Y',

        # LATIN1 LOWERCASE
        u'å': u'a',
        u'ä': u'a',
        u'ö': u'o',
        u'à': u'a',
        u'á': u'a',
        u'â': u'a',
        u'ã': u'a',
        u'ä': u'a',
        u'å': u'a',
        u'æ': u'ae',
        u'ç': u'c',
        u'è': u'e',
        u'é': u'e',
        u'ê': u'e',
        u'ë': u'e',
        u'ì': u'i',
        u'í': u'i',
        u'î': u'i',
        u'ï': u'i',
        u'ð': u'd',
        u'ñ': u'n',
        u'ò': u'o',
        u'ó': u'o',
        u'ô': u'o',
        u'õ': u'o',
        u'ö': u'o',
        u'ø': u'o',
        u'ù': u'u',
        u'ú': u'u',
        u'û': u'u',
        u'ü': u'u',
        u'ý': u'y',
        u'ÿ': u'y',

        # CYRILLIC LOWERCASE
        u'ï': u'I',
        u'э': u'E',
        u'и': u'I',
        u'й': u'I',
        u'я': u'IA',
        u'ё': u'E',
        u'ю': u'IU',
        u'ы': u'Y',
        u'ь': u'J',
        u'ш': u'SH',
        u'щ': u'SHCH',
        u'ч': u'CH',
        u'ц': u'TS',
        u'т': u'T',
        u'ж': u'ZH',
        u'з': u'Z',
        u'к': u'K',
        u'б': u'B',
        u'м': u'M',
        u'в': u'V',
        u'н': u'N',
        u'г': u'G',
        u'п': u'P',
        u'д': u'D',
        u'р': u'R',
        u'л': u'L',
        u'ф': u'F',

        # CYRILLIC UPPERCASE
        u'Ï': u'I',
        u'Х': u'X',
        u'У': u'Y',
        u'A': u'A',
        u'О': u'O',
        u'Э': u'E',
        u'И': u'I',
        u'Й': u'I',
        u'Я': u'IA',
        u'Ё': u'E',
        u'Ю': u'IU',
        u'Ы': u'Y',
        u'Ь': u'J',
        u'Ш': u'SH',
        u'Щ': u'SHCH',
        u'Ч': u'CH',
        u'Ц': u'TS',
        u'Т': u'T',
        u'Ж': u'ZH',
        u'З': u'Z',
        u'E': u'E',
        u'К': u'K',
        u'С': u'C',
        u'Б': u'B',
        u'М': u'M',
        u'В': u'V',
        u'Н': u'N',
        u'Г': u'G',
        u'П': u'P',
        u'Д': u'D',
        u'Р': u'R',
        u'Л': u'L',
        u'Ф': u'F',
    }

    for bad, good in mstrs.items():
        string = string.replace(bad, good)

    return (mangled_with_dots if accept_dots else mangled).sub('', string)
