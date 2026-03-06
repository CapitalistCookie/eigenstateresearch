from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_url: str = "redis://10.228.0.3:6379"
    # Git repos for internal docs sync (on GCE VM via sparse checkout)
    git_repos: list[str] = [
        "https://github.com/CapitalistCookie/eigenstateresearch.git",
        "https://github.com/CapitalistCookie/quanta-ai.git",
    ]
    git_repo_dir: str = "/home/user/repo-sync"

    # Topic filters
    arxiv_categories: list[str] = [
        "q-fin.TR", "q-fin.ST", "q-fin.CP", "q-fin.MF",
        "q-fin.PM", "q-fin.RM", "q-fin.GN", "stat.ML", "cs.CE",
    ]
    keywords: list[str] = [
        "algorithmic trading", "market microstructure", "order flow",
        "cumulative delta", "mean reversion", "momentum", "volatility",
        "futures trading", "commodity futures", "term structure",
        "carry trade", "seasonality", "backwardation", "contango",
        "machine learning trading", "risk management", "position sizing",
        "gold futures", "silver futures", "copper futures",
        "crude oil", "natural gas", "cotton futures",
        "grain futures", "corn futures", "soybean", "wheat futures",
        "high frequency trading", "execution algorithms",
    ]

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
