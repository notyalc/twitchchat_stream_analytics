from pathlib import Path
import yaml

class grab_config():

    def __init__(
            self,
            path: str
    ):
        yaml = '.yaml'
        properties = '.properties'
        
        self.path = path
        self.yaml = yaml
        self.properties = properties

    def check_file_path(self, rep: str):
        """
        Check if a given file path exists

        Args:
            rep: string to replace '.py'
        """
        file_path = self.path.replace('.py', rep)
        if Path(file_path).exists():
            return file_path
        else:
            raise Exception(f'Missing {file_path} file.')

    def grab_yaml(self):
        yaml_file_path = self.check_file_path(rep = self.yaml)
        with open(yaml_file_path) as yaml_file:
            config = yaml.safe_load(yaml_file)
            return config
        
    def read_ccloud_config(self):
        conf = {}
        config_file_path = self.check_file_path(rep = self.properties)
        with open(config_file_path) as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    conf[parameter] = value.strip()
        return conf