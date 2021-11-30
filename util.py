import os


class Util:
    @staticmethod
    def create_directory(dir_name):
        # Create the directory
        try:
            # Create target Directory
            os.makedirs(dir_name)
            #log.debug("Directory %s created" % dir_name)
        except FileExistsError:
            #log.warning("Directory %s already exist" % dir_name)
            pass

    @staticmethod
    def check_file_exist(file_path):
        return os.path.isfile(file_path)

    @staticmethod
    def str_to_float(float_str):
        try:
            if float_str is not None:
                float_str = ''.join(float_str.split())
                float_str = '.'.join(float_str.split(','))
                return float(float_str)
        except ValueError:
            return float("nan")
        return float("nan")
