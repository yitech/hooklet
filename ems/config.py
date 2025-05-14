#!/usr/bin/env python3
"""
Configuration loader for the EMS (Exchange Management System) module.

This module provides functionality to load and parse the YAML configuration file
that contains exchange API credentials and other settings.
"""

import logging
import os
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)

# Default path to the configuration file
DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.yml")


class ConfigError(Exception):
    """Exception raised for configuration errors."""


class Account:
    """
    Class representing an exchange account with API credentials.
    """

    def __init__(self, name: str, exchange: str, api_key: str, api_secret: str):
        """
        Initialize an account object.

        Args:
            name: Account name
            exchange: Exchange name (e.g., Binance, Coinbase)
            api_key: Exchange API key
            api_secret: Exchange API secret
        """
        self.name = name
        self.exchange = exchange
        self.api_key = api_key
        self.api_secret = api_secret

    def __str__(self) -> str:
        """String representation with masked credentials."""
        masked_key = f"{self.api_key[:5]}..." if self.api_key else "None"
        masked_secret = f"{self.api_secret[:5]}..." if self.api_secret else "None"
        return (
            f"Account(name='{self.name}', exchange='{self.exchange}', "
            f"api_key='{masked_key}', api_secret='{masked_secret}')"
        )

    def to_dict(self) -> Dict[str, str]:
        """Convert account to dictionary format."""
        return {
            "name": self.name,
            "exchange": self.exchange,
            "api_key": self.api_key,
            "api_secret": self.api_secret,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> "Account":
        """Create an Account instance from a dictionary."""
        return cls(
            name=data.get("name", ""),
            exchange=data.get("exchange", ""),
            api_key=data.get("api_key", ""),
            api_secret=data.get("api_secret", ""),
        )


class ConfigManager:
    """
    Class for managing the EMS configuration.
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the configuration manager.

        Args:
            config_path: Path to the configuration file. If None, uses the default path.
        """
        self.config_path = config_path or DEFAULT_CONFIG_PATH
        self._config = {}
        self._accounts = []
        self._loaded = False

    def load(self) -> "ConfigManager":
        """
        Load the configuration from the YAML file.

        Returns:
            Self for method chaining.

        Raises:
            ConfigError: If the configuration file cannot be loaded or parsed.
        """
        try:
            with open(self.config_path, "r") as f:
                self._config = yaml.safe_load(f)

            # Parse accounts
            self._accounts = []
            if "accounts" in self._config:
                for account_data in self._config["accounts"]:
                    self._accounts.append(Account.from_dict(account_data))

            self._loaded = True
            logger.debug(f"Configuration loaded from {self.config_path}")
            return self
        except Exception as e:
            raise ConfigError(f"Failed to load configuration from {self.config_path}: {str(e)}")

    @property
    def accounts(self) -> List[Account]:
        """Get all accounts in the configuration."""
        if not self._loaded:
            self.load()
        return self._accounts

    def get_account(self, name: str) -> Account:
        """
        Get an account by name.

        Args:
            name: Name of the account to find.

        Returns:
            Account object.

        Raises:
            ConfigError: If the account is not found.
        """
        if not self._loaded:
            self.load()

        for account in self._accounts:
            if account.name == name:
                return account

        raise ConfigError(f"Account '{name}' not found in configuration")

    def get_account_credentials(self, name: str) -> Dict[str, str]:
        """
        Get the credentials for an account.

        Args:
            name: Name of the account.

        Returns:
            Dictionary with api_key and api_secret.

        Raises:
            ConfigError: If the account is not found.
        """
        account = self.get_account(name)
        return {"api_key": account.api_key, "api_secret": account.api_secret}

    def list_account_names(self) -> List[str]:
        """
        List all account names.

        Returns:
            List of account names.
        """
        if not self._loaded:
            self.load()
        return [account.name for account in self._accounts]

    def get_raw_config(self) -> Dict[str, Any]:
        """
        Get the raw configuration dictionary.

        Returns:
            The loaded configuration dictionary.
        """
        if not self._loaded:
            self.load()
        return self._config
