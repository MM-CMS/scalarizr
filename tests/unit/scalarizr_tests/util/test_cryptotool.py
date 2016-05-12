from scalarizr.util import cryptotool


def test_encrypt_decrypt_bollard():
    text = 'A person who never made a mistake never tried anything new'
    key = 'The difference between stupidity and genius is that genius has its limits'

    assert cryptotool.decrypt_bollard(cryptotool.encrypt_bollard(text, key), key) == text
