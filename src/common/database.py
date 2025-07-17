from sqlalchemy import create_engine


def create_db_engine():
    return create_engine("postgresql://postgres:mysecretpassword@localhost/myapp")
