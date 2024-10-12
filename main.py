from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def root() -> dict:
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str) -> dict:
    return {"message": f"Hello {name}"}
