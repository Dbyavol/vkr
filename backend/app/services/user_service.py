from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.user import User
from app.schemas.auth import AdminStats, UserCreate
from app.services.security import hash_password

settings = get_settings()


def get_user_by_email(db: Session, email: str) -> User | None:
    return db.scalar(select(User).where(User.email == email.lower()))


def get_user(db: Session, user_id: int) -> User | None:
    return db.get(User, user_id)


def create_user(db: Session, payload: UserCreate, role: str = "user") -> User:
    user = User(
        email=payload.email.lower(),
        full_name=payload.full_name,
        password_hash=hash_password(payload.password),
        role=role,
        is_active=True,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def mark_login(db: Session, user: User) -> None:
    user.last_login_at = datetime.now(timezone.utc)
    db.commit()


def bootstrap_admin(db: Session) -> None:
    if get_user_by_email(db, settings.bootstrap_admin_email):
        return
    create_user(
        db,
        UserCreate(
            email=settings.bootstrap_admin_email,
            full_name="System Administrator",
            password=settings.bootstrap_admin_password,
        ),
        role="admin",
    )


def list_users(db: Session) -> list[User]:
    return list(db.scalars(select(User).order_by(User.id.desc())))


def admin_stats(db: Session) -> AdminStats:
    users_total = db.scalar(select(func.count()).select_from(User)) or 0
    admins_total = db.scalar(select(func.count()).select_from(User).where(User.role == "admin")) or 0
    active_users_total = db.scalar(select(func.count()).select_from(User).where(User.is_active.is_(True))) or 0
    return AdminStats(
        users_total=users_total,
        admins_total=admins_total,
        active_users_total=active_users_total,
    )
