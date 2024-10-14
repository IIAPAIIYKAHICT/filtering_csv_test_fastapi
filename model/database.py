from sqlalchemy import Column, ForeignKey, Integer, String, Text, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

DATABASE_URL = "sqlite:///./chat_history.db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class ChatRoom(Base):
    __tablename__ = "chat_rooms"
    id = Column(Integer, primary_key=True, index=True)
    room_name = Column(String, unique=True, index=True)
    messages = relationship("ChatMessage", back_populates="room", cascade="all, delete-orphan")


class ChatMessage(Base):
    __tablename__ = "chat_messages"
    id = Column(Integer, primary_key=True, index=True)
    user_message = Column(Text)
    bot_response = Column(Text)
    room_id = Column(Integer, ForeignKey("chat_rooms.id"))
    room = relationship("ChatRoom", back_populates="messages")


Base.metadata.create_all(bind=engine)
