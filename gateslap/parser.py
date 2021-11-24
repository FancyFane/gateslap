import configparser
import os

class ConfigFile():
    def __init__(self, filename, allow_dupe_keys=False):
        self.filename = filename
        # Note: special dictionary and non-strict to allow duplicate key values
        if allow_dupe_keys:
            self.config = configparser.ConfigParser(allow_no_value=True, dict_type=MultiOrderedDict, strict=False)
        else:
            self.config = configparser.ConfigParser(allow_no_value=True)
        os.stat(filename)
        self.config.read(filename)

    def __iter__(self):
        return self.config.__iter__()

    def __getitem__(self, key):
        if key not in self.config:
            return dict()
        return dict(self.config[key])

    def __contains__(self, value):
        return self.config.__contains__(value)

    def items(self):
        return self.config.items()

    def get_section(self, section):
        '''Returns an INI configfile section as bare string'''
        section_tag = '[' + section + ']'
        section_raw = ''
        in_section = False
        for line in open(self.filename, 'r').readlines():
            if in_section and line.startswith("["):
                in_section = False
                return section_raw
            if in_section:
                section_raw += line
            elif line.startswith('[') and line.strip() == section_tag:
                in_section = True
        return section_raw
