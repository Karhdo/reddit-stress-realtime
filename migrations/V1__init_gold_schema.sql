CREATE TABLE IF NOT EXISTS public.gold_reddit_posts (
  id BIGSERIAL PRIMARY KEY,
  post_id TEXT UNIQUE,
  subreddit TEXT,
  created_utc TIMESTAMPTZ,
  dt DATE,
  title TEXT,
  text TEXT,
  interaction_rate DOUBLE PRECISION,
  score_stress DOUBLE PRECISION,
  label_stress INT,
  permalink TEXT,
  feature_version TEXT,
  model_version TEXT,
  ingest_ts TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gold_dt ON public.gold_reddit_posts(dt);
CREATE INDEX IF NOT EXISTS idx_gold_subreddit ON public.gold_reddit_posts(subreddit);