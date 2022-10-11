
import qpt_stress_test.core.config as config

def test_loading_env():
    # Arrange

    # Act
    postgres_url = config.POSTGRES_URL

    # Assert
    assert postgres_url
