from sqlalchemy.orm import Session, joinedload

from model.database import ChatMessage, ChatRoom, SessionLocal


def add_message_to_db(room_name: str, user_message: str, bot_response: str) -> None:
    db: Session = SessionLocal()
    room = db.query(ChatRoom).filter(ChatRoom.room_name == room_name).first()
    if not room:
        room = ChatRoom(room_name=room_name)
        db.add(room)
        db.commit()
    chat_message = ChatMessage(user_message=user_message, bot_response=bot_response, room=room)
    db.add(chat_message)
    db.commit()
    db.close()


def get_chat_history(room_name: str):
    db = SessionLocal()
    room = db.query(ChatRoom).options(joinedload(ChatRoom.messages)).filter(ChatRoom.room_name == room_name).first()
    db.close()
    return room.messages if room else []


def get_all_rooms():
    db: Session = SessionLocal()
    rooms = db.query(ChatRoom).all()
    db.close()
    return rooms


def delete_room(room_name: str) -> None:
    db: Session = SessionLocal()
    room = db.query(ChatRoom).filter(ChatRoom.room_name == room_name).first()
    if room:
        db.delete(room)
        db.commit()
    db.close()
