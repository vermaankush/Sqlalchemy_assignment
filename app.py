from fastapi import FastAPI, Depends
from pydantic import BaseModel
from ollama import AsyncClient
from contextlib import asynccontextmanager
from sqlalchemy import Column, Integer, String, select, update, delete, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from database import sessionmanager, get_db_session

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Function that handles startup and shutdown events.
    To understand more, read https://fastapi.tiangolo.com/advanced/events/
    """
    yield
    if sessionmanager._engine is not None:
        # Close the DB connection
        await sessionmanager.close()


connection_string = "postgresql+asyncpg://postgres:ankush1234@localhost:5432/JK_tech_assignment"

engine = create_async_engine(connection_string, echo=True)
SessionLocal = async_sessionmaker(engine)

class Base(DeclarativeBase):
    pass

class Books_table(Base):
    __tablename__ = "books"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    title: Mapped[str] = mapped_column(index=True, nullable=False)
    author: Mapped[str] = mapped_column(index=True, nullable=False)
    genre: Mapped[str] = mapped_column(index=True, nullable=False)
    year_published: Mapped[str] = mapped_column(index=True, nullable=False)
    book_content: Mapped[str] = mapped_column(index=True, nullable=False)
    book_summary: Mapped[str] = mapped_column(index=True, nullable=True)

class Reviews_table(Base):
    __tablename__ = "reviews"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, index=True)
    book_id: Mapped[int] = mapped_column(ForeignKey("books.id"), nullable=False)
    user_id: Mapped[int] = mapped_column(index=True, nullable=False)
    review_text: Mapped[str] = mapped_column(index=True, nullable=True)
    rating: Mapped[int] = mapped_column(index=True, nullable=True)


# PYDANTIC
class BookBase(BaseModel):
    id: int
    title: str
    author: str
    genre: str
    year_published: str
    book_content: str
    book_summary: str

class ReviewBase(BaseModel):
    id: int
    book_id: int
    user_id: int
    review_text: str
    rating: int

# FASTAPI
app = FastAPI(lifespan=lifespan)


@app.get("/get_all_books")
async def get_all_books(db: AsyncSession = Depends(get_db_session)):
    statement = select(Books_table)
    results = await db.execute(statement)
    books_rows = results.scalars().all()
    return {"BOOKS": books_rows}

@app.get("/get_book_with_id")
async def get_book_with_id(book_id = int, db: AsyncSession = Depends(get_db_session)):
    statement = select(Books_table).where(Books_table.id == int(book_id))
    results = await db.execute(statement)
    books_rows = results.scalars().all()
    return {"BOOKS": books_rows}

@app.post("/add_new_book")
async def add_new_book(books: BookBase, db: AsyncSession = Depends(get_db_session)):
    db_user = Books_table(id=books.id, title=books.title, author=books.author, genre=books.genre, year_published=books.year_published, book_content=books.book_content, book_summary=books.book_summary)
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    return db_user

@app.put("/update_book_info")
async def update_book_info(book_id: int, book_contents: str, db: AsyncSession = Depends(get_db_session)):
    statement = update(Books_table).where(Books_table.id == int(book_id)).values(book_content=str(book_contents))
    await db.execute(statement)
    await db.commit()
    return "Updated content successfully"

@app.delete("/delete_book_by_id")
async def delete_book_by_id(book_id: int, db: AsyncSession = Depends(get_db_session)):
    statement = delete(Books_table).where(Books_table.id == int(book_id))
    await db.execute(statement)
    await db.commit()
    return f"Deleted book with ID = {book_id} successfully"

@app.post("/add_new_review")
async def add_new_review(review_: ReviewBase, db: AsyncSession = Depends(get_db_session)):
    db_user = Reviews_table(id=review_.id, book_id=review_.book_id, user_id=review_.user_id, review_text=review_.review_text, rating=review_.rating)
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    return db_user

@app.get("/get_reviews_with_id")
async def get_reviews_with_id(book_id_ = int, db: AsyncSession = Depends(get_db_session)):
    statement = select(Reviews_table).where(Reviews_table.book_id == int(book_id_))
    results = await db.execute(statement)
    books_rows = results.scalars().all()
    return {"BOOKS": books_rows}

@app.get("/get_summary_with_id")
async def get_summary_with_id(book_id = int, db: AsyncSession = Depends(get_db_session)):
    statement = select(Books_table.book_summary).where(Books_table.id == int(book_id))
    results = await db.execute(statement)
    books_rows = results.scalars().all()
    return {"BOOKS": books_rows}

@app.put("/update_book_summary")
async def update_book_info(book_id: int, db: AsyncSession = Depends(get_db_session)):
    statement = select(Books_table.book_content).where(Books_table.id == int(book_id))
    results = await db.execute(statement)
    book_content_ = results.scalars().all()
    response_ = await AsyncClient().generate(model='llama3', prompt=f" Write a summary of the content of the book below in 100 words:\n {book_content_}")
    generated_summary = str(response_["response"]).replace("\'", "").replace("Here is a summary of the book in 100 words:\n\n", "").replace("Here is a summary of the content in 100 words:\n\n", "")

    statement = update(Books_table).where(Books_table.id == int(book_id)).values(book_summary=str(generated_summary))
    await db.execute(statement)
    await db.commit()
    return {"BOOK_ID": book_id, "SUMMARY": generated_summary}