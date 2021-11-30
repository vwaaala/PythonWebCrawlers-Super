from tempfile import NamedTemporaryFile
from util import Util
import logutil
import logging
import shutil
import csv


log = logging.getLogger("csv_manager")
logutil.init_log(log, logging.DEBUG)


class CsvManager:

    def __init__(self, file_name, fields_type_tuples_list, identity_field):
        self.file_name = file_name
        self.fields_to_type = {field_name: field_type
                               for field_name, field_type in fields_type_tuples_list}
        self.fields_names = [field[0] for field in fields_type_tuples_list]

        self.identity_field = identity_field
        self.updated_rows = set()

        self.csv_file = None
        self.temp_file = None
        self.reader = None
        self.writer = None
        # if the csv file does not exist append directly to it
        self.no_check = True

    def _fix_row_types(self, row_dict):
        for field_name, field_value in row_dict.items():
            if field_value:
                row_dict[field_name] = self.fields_to_type.get(field_name)(field_value)

    def _fix_row_key_type(self, row_dict, key):
        if key not in self.fields_names:
            log.warning("Key: %s is not in csv filed names")

        if row_dict.get(key) is not None:
            row_dict[key] = self.fields_to_type.get(key)(row_dict.get(key))

    def open_file(self):
        if Util.check_file_exist(self.file_name):
            self.csv_file = open(self.file_name, "r", encoding="utf-8")
            self.temp_file = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
            self.reader = csv.DictReader(self.csv_file, fieldnames=self.fields_names,
                                         delimiter=",", quoting=csv.QUOTE_NONNUMERIC)
            self.writer = csv.DictWriter(self.temp_file, fieldnames=self.fields_names,
                                         delimiter=",", quoting=csv.QUOTE_NONNUMERIC)
            self.writer.writeheader()
            self.no_check = False
        else:
            self.csv_file = open(self.file_name, "w", encoding="utf-8")
            self.writer = csv.DictWriter(self.csv_file, fieldnames=self.fields_names,
                                         delimiter=",", quoting=csv.QUOTE_NONNUMERIC)
            self.writer.writeheader()
            self.no_check = True

    def close_file(self):
        if self.csv_file is not None:
            if not self.no_check:
                # start from beginning and skip the header
                self.csv_file.seek(0)
                next(self.reader)
                for row in self.reader:
                    self._fix_row_types(row)
                    if row.get(self.identity_field) in self.updated_rows:
                        # log.debug("row: %s already was updated" % row)
                        pass
                    else:
                        # log.debug("need to add row %s" % row)
                        self.writer.writerow(row)

                self.temp_file.flush()
                self.csv_file.close()

                if self.temp_file is not None:
                    shutil.move(self.temp_file.name, self.file_name)

    def update_row(self, row_dict):
        if self.no_check:
            self._fix_row_types(row_dict)
            self.writer.writerow(row_dict)
        else:
            if self.check_row_exist(row_dict):
                self.updated_rows.add(row_dict.get(self.identity_field))
            self._fix_row_types(row_dict)
            self.writer.writerow(row_dict)

    def check_row_exist(self, row_dict):
        if self.no_check:
            return False
        else:
            # start from beginning and skip the header
            self.csv_file.seek(0)
            next(self.reader)
            for row in self.reader:
                self._fix_row_key_type(row, self.identity_field)
                self._fix_row_key_type(row_dict, self.identity_field)
                if row.get(self.identity_field) == row_dict.get(self.identity_field):
                    return True
            return False
