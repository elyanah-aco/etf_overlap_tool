import pynecone as pc

config = pc.Config(
    app_name="etf_overlap",
    db_url="sqlite:///pynecone.db",
    env=pc.Env.DEV,
)
