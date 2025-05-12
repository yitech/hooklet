#!/usr/bin/env python3
"""
Unit tests for the configuration module.
"""

import os
import tempfile
import pytest
from ems.config import ConfigManager, Account, ConfigError

class TestAccount:
    """Test cases for the Account class."""
    
    def test_init(self):
        """Test Account initialization."""
        account = Account(name="test_account", api_key="test_key", api_secret="test_secret")
        assert account.name == "test_account"
        assert account.api_key == "test_key"
        assert account.api_secret == "test_secret"
    
    def test_string_representation(self):
        """Test string representation with masked credentials."""
        account = Account(name="test_account", api_key="test_key", api_secret="test_secret")
        str_repr = str(account)
        assert "test_account" in str_repr
        assert "test_" in str_repr
        assert "..." in str_repr
        assert "test_key" not in str_repr  # Full key should not be visible
        assert "test_secret" not in str_repr  # Full secret should not be visible
    
    def test_to_dict(self):
        """Test conversion to dictionary."""
        account = Account(name="test_account", api_key="test_key", api_secret="test_secret")
        data = account.to_dict()
        assert data == {
            'name': 'test_account',
            'api_key': 'test_key',
            'api_secret': 'test_secret'
        }
    
    def test_from_dict(self):
        """Test creation from dictionary."""
        data = {
            'name': 'test_account',
            'api_key': 'test_key',
            'api_secret': 'test_secret'
        }
        account = Account.from_dict(data)
        assert account.name == "test_account"
        assert account.api_key == "test_key"
        assert account.api_secret == "test_secret"
    
    def test_from_dict_missing_fields(self):
        """Test creation from dictionary with missing fields."""
        data = {'name': 'test_account'}
        account = Account.from_dict(data)
        assert account.name == "test_account"
        assert account.api_key == ""
        assert account.api_secret == ""


class TestConfigManager:
    """Test cases for the ConfigManager class."""
    
    @pytest.fixture
    def sample_config(self):
        """Fixture to create a temporary config file."""
        config_content = """
accounts:
  - name: "test_account1"
    api_key: "key1"
    api_secret: "secret1"
  - name: "test_account2"
    api_key: "key2"
    api_secret: "secret2"
"""
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as f:
            f.write(config_content)
            temp_config_path = f.name
        
        yield temp_config_path
        # Cleanup
        if os.path.exists(temp_config_path):
            os.unlink(temp_config_path)
    
    def test_init(self):
        """Test ConfigManager initialization."""
        config_path = "/path/to/custom/config.yml"
        cm = ConfigManager(config_path=config_path)
        assert cm.config_path == config_path
        assert not cm._loaded
        
        default_cm = ConfigManager()
        assert default_cm.config_path is not None
        assert not default_cm._loaded
    
    def test_load(self, sample_config):
        """Test loading configuration from file."""
        cm = ConfigManager(config_path=sample_config)
        cm.load()
        assert cm._loaded
        assert len(cm.accounts) == 2
        assert cm.accounts[0].name == "test_account1"
        assert cm.accounts[1].name == "test_account2"
    
    def test_load_invalid_file(self):
        """Test loading from a non-existent file."""
        cm = ConfigManager(config_path="/non/existent/file.yml")
        with pytest.raises(ConfigError):
            cm.load()
    
    def test_get_account(self, sample_config):
        """Test getting an account by name."""
        cm = ConfigManager(config_path=sample_config).load()
        account = cm.get_account("test_account1")
        assert account.name == "test_account1"
        assert account.api_key == "key1"
        assert account.api_secret == "secret1"
    
    def test_get_nonexistent_account(self, sample_config):
        """Test getting a non-existent account."""
        cm = ConfigManager(config_path=sample_config).load()
        with pytest.raises(ConfigError) as excinfo:
            cm.get_account("non_existent")
        assert "not found" in str(excinfo.value)
    
    def test_get_account_credentials(self, sample_config):
        """Test getting account credentials."""
        cm = ConfigManager(config_path=sample_config).load()
        creds = cm.get_account_credentials("test_account2")
        assert creds == {
            'api_key': 'key2',
            'api_secret': 'secret2'
        }
    
    def test_list_account_names(self, sample_config):
        """Test listing account names."""
        cm = ConfigManager(config_path=sample_config).load()
        names = cm.list_account_names()
        assert names == ["test_account1", "test_account2"]
    
    def test_get_raw_config(self, sample_config):
        """Test getting raw configuration."""
        cm = ConfigManager(config_path=sample_config).load()
        raw_config = cm.get_raw_config()
        assert 'accounts' in raw_config
        assert len(raw_config['accounts']) == 2
