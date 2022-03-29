#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import json
import os
import tempfile
from functools import reduce

"""

         888                                                  888         d8b
         888                                                  888         Y8P
         888                                                  888
 .d8888b 88888b.  888d888 .d88b.  88888b.d88b.   .d88b.   .d88888 888d888 888 888  888  .d88b.  888d888
d88P"    888 "88b 888P"  d88""88b 888 "888 "88b d8P  Y8b d88" 888 888P"   888 888  888 d8P  Y8b 888P"
888      888  888 888    888  888 888  888  888 88888888 888  888 888     888 Y88  88P 88888888 888
Y88b.    888  888 888    Y88..88P 888  888  888 Y8b.     Y88b 888 888     888  Y8bd8P  Y8b.     888
 "Y8888P 888  888 888     "Y88P"  888  888  888  "Y8888   "Y88888 888     888   Y88P    "Y8888  888   88888888

by UltrafunkAmsterdam (https://github.com/ultrafunkamsterdam)

"""

__version__ = "3.1.3"

import json
import logging
import os
import re
import shutil
import sys
import tempfile
import time

import selenium.webdriver.chrome.service
import selenium.webdriver.chrome.webdriver
import selenium.webdriver.common.service
import selenium.webdriver.remote.webdriver

from .options import ChromeOptions
from .patcher import IS_POSIX
from .patcher import Patcher
from .reactor import Reactor
from .dprocess import start_detached

import sys

platform = sys.platform
if platform.endswith("win32"):
    chromedriver_path = "./chrome/driver_linux/chromedriver"
if platform.endswith("linux"):
    chromedriver_path = "./chrome/driver_linux/chromedriver"
if platform.endswith("darwin"):
    chromedriver_path = "./chrome/driver_mac/chromedriver"

__all__ = (
    "Chrome",
    "ChromeOptions",
    "Patcher",
    "Reactor",
    "find_chrome_executable",
)

logger = logging.getLogger("uc")
logger.setLevel(logging.getLogger().getEffectiveLevel())


class Chrome(selenium.webdriver.chrome.webdriver.WebDriver):
    _instances = set()
    session_id = None
    debug = False

    def __init__(
        self,
        user_data_dir=None,
        browser_executable_path=None,
        port=0,
        options=None,
        enable_cdp_events=False,
        service_args=None,
        desired_capabilities=None,
        service_log_path=None,
        keep_alive=True,
        log_level=0,
        headless=False,
        version_main=None,
        patcher_force_close=False,
        suppress_welcome=True,
        use_subprocess=False,
        debug=False,
        **kw
    ):
        self.debug = debug
        # patcher = Patcher(
        #     executable_path=None,
        #     force=patcher_force_close,
        #     version_main=version_main,
        # )
        # patcher.auto()

        if not options:
            options = ChromeOptions()

        try:
            if hasattr(options, "_session") and options._session is not None:
                #  prevent reuse of options,
                #  as it just appends arguments, not replace them
                #  you'll get conflicts starting chrome
                raise RuntimeError("you cannot reuse the ChromeOptions object")
        except AttributeError:
            pass

        options._session = self

        debug_port = selenium.webdriver.common.service.utils.free_port()
        debug_host = "127.0.0.1"

        if not options.debugger_address:
            options.debugger_address = "%s:%d" % (debug_host, debug_port)

        if enable_cdp_events:
            options.set_capability(
                "goog:loggingPrefs", {"performance": "ALL", "browser": "ALL"}
            )

        options.add_argument("--remote-debugging-host=%s" % debug_host)
        options.add_argument("--remote-debugging-port=%s" % debug_port)

        language, keep_user_data_dir = None, bool(user_data_dir)

        # see if a custom user profile is specified in options
        for arg in options.arguments:

            if "lang" in arg:
                m = re.search("(?:--)?lang(?:[ =])?(.*)", arg)
                try:
                    language = m[1]
                except IndexError:
                    logger.debug("will set the language to en-US,en;q=0.9")
                    language = "en-US,en;q=0.9"

            if "user-data-dir" in arg:
                m = re.search("(?:--)?user-data-dir(?:[ =])?(.*)", arg)
                try:
                    user_data_dir = m[1]
                    logger.debug(
                        "user-data-dir found in user argument %s => %s" % (arg, m[1])
                    )
                    keep_user_data_dir = False

                except IndexError:
                    logger.debug(
                        "no user data dir could be extracted from supplied argument %s "
                        % arg
                    )

        options.add_argument("--lang=%s" % "en-US")

        if not options.binary_location:
            options.binary_location = (
                browser_executable_path or find_chrome_executable()
            )

        self._delay = 3

        self.user_data_dir = user_data_dir
        self.keep_user_data_dir = keep_user_data_dir

        if suppress_welcome:
            options.arguments.extend(["--no-default-browser-check", "--no-first-run"])
        if headless or options.headless:
            options.headless = True
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--start-maximized")
            options.add_argument("--no-sandbox")
            # fixes "could not connect to chrome" error when running
            # on linux using privileged user like root (which i don't recommend)

        options.add_argument(
            "--log-level=%d" % log_level
            or divmod(logging.getLogger().getEffectiveLevel(), 10)[0]
        )

        # fix exit_type flag to prevent tab-restore nag
        try:
            with open(
                os.path.join(user_data_dir, "Default/Preferences"),
                encoding="latin1",
                mode="r+",
            ) as fs:
                config = json.load(fs)
                if config["profile"]["exit_type"] is not None:
                    # fixing the restore-tabs-nag
                    config["profile"]["exit_type"] = None
                fs.seek(0, 0)
                json.dump(config, fs)
                logger.debug("fixed exit_type flag")
        except Exception as e:
            logger.debug("did not find a bad exit_type flag ")

        self.options = options

        if not desired_capabilities:
            desired_capabilities = options.to_capabilities()

        if not use_subprocess:
            self.browser_pid = start_detached(
                options.binary_location, *options.arguments
            )
        else:
            browser = subprocess.Popen(
                [options.binary_location, *options.arguments],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                close_fds=IS_POSIX,
            )
            self.browser_pid = browser.pid

        super(Chrome, self).__init__(
            executable_path= chromedriver_path,
            port=port,
            options=options,
            service_args=service_args,
            desired_capabilities=desired_capabilities,
            service_log_path=service_log_path,
            keep_alive=keep_alive,
        )

        self.reactor = None

        if enable_cdp_events:
            if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
                logging.getLogger(
                    "selenium.webdriver.remote.remote_connection"
                ).setLevel(20)
            reactor = Reactor(self)
            reactor.start()
            self.reactor = reactor

        if options.headless:
            self._configure_headless()

    def __getattribute__(self, item):

        if not super().__getattribute__("debug"):
            return super().__getattribute__(item)
        else:
            import inspect

            original = super().__getattribute__(item)
            if inspect.ismethod(original) and not inspect.isclass(original):

                def newfunc(*args, **kwargs):
                    logger.debug(
                        "calling %s with args %s and kwargs %s\n"
                        % (original.__qualname__, args, kwargs)
                    )
                    return original(*args, **kwargs)

                return newfunc
            return original

    def _configure_headless(self):

        orig_get = self.get
        logger.info("setting properties for headless")

        def get_wrapped(*args, **kwargs):
            if self.execute_script("return navigator.webdriver"):
                logger.info("patch navigator.webdriver")
                self.execute_cdp_cmd(
                    "Page.addScriptToEvaluateOnNewDocument",
                    {
                        "source": """

                            Object.defineProperty(window, 'navigator', {
                                value: new Proxy(navigator, {
                                        has: (target, key) => (key === 'webdriver' ? false : key in target),
                                        get: (target, key) =>
                                                key === 'webdriver' ?
                                                false :
                                                typeof target[key] === 'function' ?
                                                target[key].bind(target) :
                                                target[key]
                                        })
                            });

                    """
                    },
                )

                logger.info("patch user-agent string")
                self.execute_cdp_cmd(
                    "Network.setUserAgentOverride",
                    {
                        "userAgent": self.execute_script(
                            "return navigator.userAgent"
                        ).replace("Headless", "")
                    },
                )
                self.execute_cdp_cmd(
                    "Page.addScriptToEvaluateOnNewDocument",
                    {
                        "source": """
                            Object.defineProperty(navigator, 'maxTouchPoints', {
                                    get: () => 1
                            })"""
                    },
                )
            return orig_get(*args, **kwargs)

        self.get = get_wrapped

    def __dir__(self):
        return object.__dir__(self)

    def _get_cdc_props(self):
        return self.execute_script(
            """
            let objectToInspect = window,
                result = [];
            while(objectToInspect !== null)
            { result = result.concat(Object.getOwnPropertyNames(objectToInspect));
              objectToInspect = Object.getPrototypeOf(objectToInspect); }
            return result.filter(i => i.match(/.+_.+_(Array|Promise|Symbol)/ig))
            """
        )

    def _hook_remove_cdc_props(self):
        self.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                    let objectToInspect = window,
                        result = [];
                    while(objectToInspect !== null) 
                    { result = result.concat(Object.getOwnPropertyNames(objectToInspect));
                      objectToInspect = Object.getPrototypeOf(objectToInspect); }
                    result.forEach(p => p.match(/.+_.+_(Array|Promise|Symbol)/ig)
                                        &&delete window[p]&&console.log('removed',p))
                    """
            },
        )

    def get(self, url):
        if self._get_cdc_props():
            self._hook_remove_cdc_props()
        return super().get(url)

    def add_cdp_listener(self, event_name, callback):
        if (
            self.reactor
            and self.reactor is not None
            and isinstance(self.reactor, Reactor)
        ):
            self.reactor.add_event_handler(event_name, callback)
            return self.reactor.handlers
        return False

    def clear_cdp_listeners(self):
        if self.reactor and isinstance(self.reactor, Reactor):
            self.reactor.handlers.clear()

    def tab_new(self, url: str):
        """
        this opens a url in a new tab.
        apparently, that passes all tests directly!

        Parameters
        ----------
        url

        Returns
        -------

        """
        if not hasattr(self, "cdp"):
            from .cdp import CDP

            self.cdp = CDP(self.options)
        self.cdp.tab_new(url)

    def reconnect(self, timeout=0.1):
        try:
            self.service.stop()
        except Exception as e:
            logger.debug(e)
        time.sleep(timeout)
        try:
            self.service.start()
        except Exception as e:
            logger.debug(e)

        try:
            self.start_session()
        except Exception as e:
            logger.debug(e)

    def start_session(self, capabilities=None, browser_profile=None):
        if not capabilities:
            capabilities = self.options.to_capabilities()
        super(selenium.webdriver.chrome.webdriver.WebDriver, self).start_session(
            capabilities, browser_profile
        )
        # super(Chrome, self).start_session(capabilities, browser_profile)

    def quit(self):
        logger.debug("closing webdriver")
        if hasattr(self, "service") and getattr(self.service, "process", None):
            self.service.process.kill()
        try:
            if self.reactor and isinstance(self.reactor, Reactor):
                logger.debug("shutting down reactor")
                self.reactor.event.set()
        except Exception:  # noqa
            pass
        try:
            logger.debug("killing browser")
            os.kill(self.browser_pid, 15)

        except TimeoutError as e:
            logger.debug(e, exc_info=True)
        except Exception:  # noqa
            pass

        if (
            hasattr(self, "keep_user_data_dir")
            and hasattr(self, "user_data_dir")
            and not self.keep_user_data_dir
        ):
            for _ in range(5):
                try:

                    shutil.rmtree(self.user_data_dir, ignore_errors=False)
                except FileNotFoundError:
                    pass
                except (RuntimeError, OSError, PermissionError) as e:
                    logger.debug(
                        "When removing the temp profile, a %s occured: %s\nretrying..."
                        % (e.__class__.__name__, e)
                    )
                else:
                    logger.debug("successfully removed %s" % self.user_data_dir)
                    break
                time.sleep(0.1)

    def __del__(self):
        try:
            self.service.process.kill()
        except:
            pass
        self.quit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.service.stop()
        time.sleep(self._delay)
        self.service.start()
        self.start_session()

    def __hash__(self):
        return hash(self.options.debugger_address)

class ChromeWithPrefs(Chrome):
    def __init__(self, *args, options=None, **kwargs):
        if options:
            self._handle_prefs(options)

        super().__init__(*args, options=options, **kwargs)

        # remove the user_data_dir when quitting
        self.keep_user_data_dir = False
    
    @staticmethod
    def _handle_prefs(options):
        if prefs := options.experimental_options.get("prefs"):
            # turn a (dotted key, value) into a proper nested dict
            def undot_key(key, value):
                if "." in key:
                    key, rest = key.split(".", 1)
                    value = undot_key(rest, value)
                return {key: value}

            # undot prefs dict keys
            undot_prefs = reduce(
                lambda d1, d2: {**d1, **d2},  # merge dicts
                (undot_key(key, value) for key, value in prefs.items()),
            )

            # create an user_data_dir and add its path to the options
            user_data_dir = os.path.normpath(tempfile.mkdtemp())
            options.add_argument(f"--user-data-dir={user_data_dir}")

            # create the preferences json file in its default directory
            default_dir = os.path.join(user_data_dir, "Default")
            os.mkdir(default_dir)

            prefs_file = os.path.join(default_dir, "Preferences")
            with open(prefs_file, encoding="latin1", mode="w") as f:
                json.dump(undot_prefs, f)

            # pylint: disable=protected-access
            # remove the experimental_options to avoid an error
            del options._experimental_options["prefs"]

def find_chrome_executable():
    """
    Finds the chrome, chrome beta, chrome canary, chromium executable

    Returns
    -------
    executable_path :  str
        the full file path to found executable

    """
    candidates = set()
    if IS_POSIX:
        for item in os.environ.get("PATH").split(os.pathsep):
            for subitem in ("google-chrome", "chromium", "chromium-browser", "chrome"):
                candidates.add(os.sep.join((item, subitem)))
        if "darwin" in sys.platform:
            candidates.update(
                ["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"]
            )
    else:
        for item in map(
            os.environ.get, ("PROGRAMFILES", "PROGRAMFILES(X86)", "LOCALAPPDATA")
        ):
            for subitem in (
                "Google/Chrome/Application",
                "Google/Chrome Beta/Application",
                "Google/Chrome Canary/Application",
            ):
                candidates.add(os.sep.join((item, subitem, "chrome.exe")))
    for candidate in candidates:
        if os.path.exists(candidate) and os.access(candidate, os.X_OK):
            return os.path.normpath(candidate)
