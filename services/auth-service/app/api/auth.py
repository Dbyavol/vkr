from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models.user import User
from app.schemas.auth import AdminStats, LoginRequest, TokenResponse, UserCreate, UserRead
from app.services.security import create_access_token, decode_access_token, verify_password
from app.services.user_service import admin_stats, create_user, get_user, get_user_by_email, list_users, mark_login

router = APIRouter(prefix="/api/v1", tags=["auth"])


def current_user(authorization: str | None = Header(default=None), db: Session = Depends(get_db)) -> User:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1]
    try:
        payload = decode_access_token(token)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    user = get_user(db, int(payload["sub"]))
    if user is None or not user.is_active:
        raise HTTPException(status_code=401, detail="User not found or inactive")
    return user


def require_admin(user: User = Depends(current_user)) -> User:
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin role required")
    return user


@router.post("/auth/register", response_model=TokenResponse, status_code=201)
def register(payload: UserCreate, db: Session = Depends(get_db)) -> TokenResponse:
    if get_user_by_email(db, payload.email):
        raise HTTPException(status_code=409, detail="User already exists")
    user = create_user(db, payload)
    token = create_access_token(str(user.id), {"email": user.email, "role": user.role})
    return TokenResponse(access_token=token, user=UserRead.model_validate(user))


@router.post("/auth/login", response_model=TokenResponse)
def login(payload: LoginRequest, db: Session = Depends(get_db)) -> TokenResponse:
    user = get_user_by_email(db, payload.email)
    if user is None or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    mark_login(db, user)
    token = create_access_token(str(user.id), {"email": user.email, "role": user.role})
    return TokenResponse(access_token=token, user=UserRead.model_validate(user))


@router.get("/users/me", response_model=UserRead)
def me(user: User = Depends(current_user)) -> UserRead:
    return UserRead.model_validate(user)


@router.get("/admin/users", response_model=list[UserRead])
def admin_users(_: User = Depends(require_admin), db: Session = Depends(get_db)) -> list[UserRead]:
    return [UserRead.model_validate(user) for user in list_users(db)]


@router.get("/admin/stats", response_model=AdminStats)
def stats(_: User = Depends(require_admin), db: Session = Depends(get_db)) -> AdminStats:
    return admin_stats(db)
