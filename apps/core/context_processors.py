from .services import PlatformConfigService

def platform_settings(request):
    """
    Context processor to make platform config available in all templates.
    """
    return {
        'PLATFORM_CONFIG': PlatformConfigService.get_config()
    }
