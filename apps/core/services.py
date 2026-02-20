from .models import PlatformConfig

class PlatformConfigService:
    @staticmethod
    def get_config():
        """
        Return the singleton PlatformConfig instance.
        """
        return PlatformConfig.load()
