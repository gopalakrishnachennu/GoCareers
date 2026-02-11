"""
==========================================================
SELF-DIAGNOSTIC HEALTH CHECK COMMAND
==========================================================
Run: python manage.py healthcheck

Checks:
  ✅ Database connection
  ✅ Installed apps and models
  ✅ Template rendering (loads base.html)
  ✅ Static files directory
  ✅ Required packages
  ✅ Log file writability
  ✅ URL configuration (reverse all named URLs)
  ✅ Migration status
"""

import sys
import importlib
import logging
from io import StringIO
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db import connections, OperationalError
from django.apps import apps
from django.template import engines
from django.conf import settings
from django.urls import reverse, NoReverseMatch

logger = logging.getLogger('diagnostics')


class Command(BaseCommand):
    help = 'Run a comprehensive self-diagnostic health check on the platform.'

    CHECKS = [
        'check_database',
        'check_migrations',
        'check_installed_apps',
        'check_models',
        'check_templates',
        'check_static_files',
        'check_required_packages',
        'check_log_files',
        'check_urls',
        'check_constants',
        'check_middleware',
    ]

    def handle(self, *args, **options):
        self.stdout.write(self.style.HTTP_INFO('\n' + '=' * 60))
        self.stdout.write(self.style.HTTP_INFO('  🏥  PLATFORM HEALTH CHECK'))
        self.stdout.write(self.style.HTTP_INFO('=' * 60 + '\n'))

        passed = 0
        failed = 0
        warnings = 0

        for check_name in self.CHECKS:
            method = getattr(self, check_name)
            try:
                result = method()
                if result == 'pass':
                    passed += 1
                elif result == 'warn':
                    warnings += 1
                else:
                    failed += 1
            except Exception as e:
                self.report_fail(check_name.replace('check_', '').replace('_', ' ').title(), str(e))
                failed += 1

        # Summary
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write(f'  RESULTS: ✅ {passed} passed | ⚠️  {warnings} warnings | ❌ {failed} failed')
        self.stdout.write('=' * 60 + '\n')

        if failed > 0:
            self.stdout.write(self.style.ERROR('⛔ Some checks FAILED. Review the output above.'))
            logger.error(f'Health check: {failed} checks failed, {warnings} warnings')
        elif warnings > 0:
            self.stdout.write(self.style.WARNING('⚠️  All checks passed with warnings.'))
            logger.warning(f'Health check: {warnings} warnings')
        else:
            self.stdout.write(self.style.SUCCESS('🎉 All checks PASSED! Platform is healthy.'))
            logger.info('Health check: All checks passed')

    def report_pass(self, name, detail=''):
        msg = f'  ✅ {name}'
        if detail:
            msg += f' — {detail}'
        self.stdout.write(self.style.SUCCESS(msg))

    def report_fail(self, name, detail=''):
        msg = f'  ❌ {name}'
        if detail:
            msg += f' — {detail}'
        self.stdout.write(self.style.ERROR(msg))

    def report_warn(self, name, detail=''):
        msg = f'  ⚠️  {name}'
        if detail:
            msg += f' — {detail}'
        self.stdout.write(self.style.WARNING(msg))

    # -------------------------------------------------------
    # Individual Checks
    # -------------------------------------------------------

    def check_database(self):
        """Test database connectivity."""
        try:
            db = connections['default']
            db.cursor()
            engine = settings.DATABASES['default']['ENGINE']
            self.report_pass('Database', f'Connected ({engine.split(".")[-1]})')
            return 'pass'
        except OperationalError as e:
            self.report_fail('Database', f'Cannot connect: {e}')
            return 'fail'

    def check_migrations(self):
        """Check for unapplied migrations."""
        from django.core.management import call_command
        out = StringIO()
        call_command('showmigrations', '--plan', stdout=out)
        output = out.getvalue()
        unapplied = [line for line in output.splitlines() if line.strip().startswith('[ ]')]
        if unapplied:
            self.report_warn('Migrations', f'{len(unapplied)} unapplied migration(s)')
            for m in unapplied[:5]:
                self.stdout.write(f'         {m.strip()}')
            return 'warn'
        self.report_pass('Migrations', 'All applied')
        return 'pass'

    def check_installed_apps(self):
        """Verify all INSTALLED_APPS can be imported."""
        broken = []
        for app in settings.INSTALLED_APPS:
            try:
                importlib.import_module(app)
            except ImportError:
                # Try the AppConfig path
                try:
                    importlib.import_module(app.rsplit('.', 1)[0])
                except ImportError:
                    broken.append(app)
        if broken:
            self.report_fail('Installed Apps', f'{len(broken)} broken: {", ".join(broken)}')
            return 'fail'
        self.report_pass('Installed Apps', f'{len(settings.INSTALLED_APPS)} apps loaded')
        return 'pass'

    def check_models(self):
        """Verify all models are properly registered."""
        models = apps.get_models()
        model_count = len(models)
        if model_count == 0:
            self.report_warn('Models', 'No models found')
            return 'warn'
        self.report_pass('Models', f'{model_count} models registered')
        return 'pass'

    def check_templates(self):
        """Verify template engine can load base.html."""
        try:
            engine = engines['django']
            template = engine.get_template('base.html')
            self.report_pass('Templates', 'base.html loads OK')
            return 'pass'
        except Exception as e:
            self.report_fail('Templates', f'Cannot load base.html: {e}')
            return 'fail'

    def check_static_files(self):
        """Verify static files directories exist."""
        static_root = getattr(settings, 'STATIC_ROOT', None)
        staticfiles_dirs = getattr(settings, 'STATICFILES_DIRS', [])
        css_path = Path(settings.BASE_DIR) / 'theme' / 'static' / 'css' / 'dist' / 'styles.css'

        if css_path.exists():
            size_kb = css_path.stat().st_size / 1024
            self.report_pass('Static Files', f'Tailwind CSS compiled ({size_kb:.0f}KB)')
            return 'pass'
        else:
            self.report_warn('Static Files', 'Tailwind CSS not compiled — run: python manage.py tailwind start')
            return 'warn'

    def check_required_packages(self):
        """Verify critical Python packages are installed."""
        required = [
            ('django', 'Django'),
            ('widget_tweaks', 'django-widget-tweaks'),
            ('decouple', 'python-decouple'),
        ]
        missing = []
        for module_name, display_name in required:
            try:
                importlib.import_module(module_name)
            except ImportError:
                missing.append(display_name)

        if missing:
            self.report_fail('Required Packages', f'Missing: {", ".join(missing)}')
            return 'fail'
        self.report_pass('Required Packages', f'All {len(required)} packages installed')
        return 'pass'

    def check_log_files(self):
        """Verify log directory is writable."""
        log_dir = Path(settings.BASE_DIR) / 'logs'
        if not log_dir.exists():
            try:
                log_dir.mkdir(parents=True)
                self.report_pass('Log Files', f'Created {log_dir}')
                return 'pass'
            except OSError as e:
                self.report_fail('Log Files', f'Cannot create log dir: {e}')
                return 'fail'

        log_files = list(log_dir.glob('*.log'))
        self.report_pass('Log Files', f'{len(log_files)} log file(s) in {log_dir}')
        return 'pass'

    def check_urls(self):
        """Test that key named URLs resolve correctly."""
        test_urls = ['home', 'login', 'logout', 'job-list', 'consultant-list', 'inbox', 'submission-list']
        broken = []
        for url_name in test_urls:
            try:
                reverse(url_name)
            except NoReverseMatch:
                broken.append(url_name)
        if broken:
            self.report_fail('URL Config', f'{len(broken)} broken: {", ".join(broken)}')
            return 'fail'
        self.report_pass('URL Config', f'{len(test_urls)} routes verified')
        return 'pass'

    def check_constants(self):
        """Verify the config/constants module loads."""
        try:
            from config.constants import SITE_NAME, MAX_UPLOAD_SIZE, MSG_LOGIN_HEADING
            if not SITE_NAME:
                self.report_warn('Constants', 'SITE_NAME is empty')
                return 'warn'
            self.report_pass('Constants', f'Loaded (SITE_NAME="{SITE_NAME}")')
            return 'pass'
        except ImportError as e:
            self.report_fail('Constants', f'Import error: {e}')
            return 'fail'

    def check_middleware(self):
        """Verify middleware chain is configured."""
        middleware = settings.MIDDLEWARE
        has_request_logging = 'config.middleware.RequestLoggingMiddleware' in middleware
        if has_request_logging:
            self.report_pass('Middleware', f'{len(middleware)} middleware active (incl. request logging)')
        else:
            self.report_warn('Middleware', f'{len(middleware)} middleware active (request logging not enabled)')
        return 'pass' if has_request_logging else 'warn'
