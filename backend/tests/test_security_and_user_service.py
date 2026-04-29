from app.schemas.auth import UserCreate
from app.services.security import create_access_token, decode_access_token, hash_password, verify_password
from app.services.user_service import create_user, get_user_by_email, list_users


def test_security_hash_and_token_roundtrip():
    password_hash = hash_password("secret123")

    assert verify_password("secret123", password_hash)
    assert not verify_password("other", password_hash)

    token = create_access_token("42", {"email": "user@example.com", "role": "user"})
    payload = decode_access_token(token)
    assert payload["sub"] == "42"
    assert payload["email"] == "user@example.com"


def test_user_service_creates_and_lists_users(db):
    created = create_user(
        db,
        UserCreate(email="user@example.com", full_name="Test User", password="secret123"),
    )

    assert created.email == "user@example.com"
    assert get_user_by_email(db, "user@example.com") is not None
    assert len(list_users(db)) >= 2  # bootstrap admin + created user
