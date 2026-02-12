from .models import PlatformConfig

class PlatformConfigService:
    @staticmethod
    def get_config():
        """
        Return the singleton PlatformConfig instance.
        """
        return PlatformConfig.load()

    @staticmethod
    def is_feature_enabled(feature_name):
        """
        Check if a specific feature flag is enabled.
        """
        config = PlatformConfig.load()
        return getattr(config, feature_name, False)
