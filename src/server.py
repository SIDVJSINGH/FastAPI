import uvicorn
from multiprocessing import cpu_count, freeze_support


def start_server(host="0.0.0.0",
                 port=5000,
                 num_workeprs=4,
                 loop="asyncio",
                 reload=True):
    uvicorn.run("app.main:app",
                host=host,
                port=port,
                workers=num_workers,
                loop=loop,
                reload=reload)


if __name__ == "__main__":
    freeze_support()  # Needed for pyinstaller for multiprocessing on WindowsOS
    num_workers = int(cpu_count() * 0.75)
    start_server(num_workeprs=num_workers)