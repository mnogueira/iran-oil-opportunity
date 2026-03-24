import os
import unittest

from iran_oil_opportunity.local_news import (
    NewsSource,
    fetch_recent_headlines_with_status,
)


class _FakeResponse:
    def __init__(self, *, text: str = "", json_payload: dict | None = None, status_code: int = 200) -> None:
        self.text = text
        self._json_payload = json_payload or {}
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"status={self.status_code}")

    def json(self) -> dict:
        return self._json_payload


class _FakeSession:
    def __init__(self, responses: dict[str, _FakeResponse]) -> None:
        self.responses = responses

    def get(self, url: str, **_kwargs) -> _FakeResponse:
        response = self.responses.get(url)
        if response is None:
            raise RuntimeError(f"unexpected_url={url}")
        return response


class LocalNewsTests(unittest.TestCase):
    def test_fetch_recent_headlines_supports_rss_and_newsapi(self) -> None:
        rss_url = "https://example.com/rss"
        newsapi_url = "https://example.com/newsapi"
        rss_body = """
        <rss>
          <channel>
            <item>
              <title>Iran oil war risk climbs</title>
              <link>https://example.com/rss-story</link>
              <pubDate>Tue, 24 Mar 2026 18:00:00 GMT</pubDate>
            </item>
          </channel>
        </rss>
        """
        session = _FakeSession(
            {
                rss_url: _FakeResponse(text=rss_body),
                newsapi_url: _FakeResponse(
                    json_payload={
                        "articles": [
                            {
                                "title": "Reuters says Brent rallies on Iran risk",
                                "url": "https://example.com/newsapi-story",
                                "publishedAt": "2026-03-24T18:05:00Z",
                            }
                        ]
                    }
                ),
            }
        )
        previous_value = os.environ.get("NEWSAPI_TEST_KEY")
        os.environ["NEWSAPI_TEST_KEY"] = "test-key"
        try:
            headlines, statuses = fetch_recent_headlines_with_status(
                sources=(
                    NewsSource("RSS", "en", rss_url),
                    NewsSource(
                        "NewsAPI",
                        "en",
                        newsapi_url,
                        source_type="newsapi",
                        query="iran oil",
                        api_key_env="NEWSAPI_TEST_KEY",
                    ),
                ),
                session=session,
            )
        finally:
            if previous_value is None:
                os.environ.pop("NEWSAPI_TEST_KEY", None)
            else:
                os.environ["NEWSAPI_TEST_KEY"] = previous_value
        self.assertEqual(len(headlines), 2)
        self.assertTrue(all(status.ok for status in statuses))

    def test_fetch_recent_headlines_reports_missing_api_key(self) -> None:
        headlines, statuses = fetch_recent_headlines_with_status(
            sources=(
                NewsSource(
                    "X",
                    "en",
                    "https://api.x.com/2/tweets/search/recent",
                    source_type="x_recent_search",
                    query="iran oil",
                    api_key_env="MISSING_X_TOKEN",
                ),
            ),
            session=_FakeSession({}),
        )
        self.assertEqual(headlines, [])
        self.assertEqual(len(statuses), 1)
        self.assertFalse(statuses[0].ok)
        self.assertIn("Missing MISSING_X_TOKEN", statuses[0].error or "")


if __name__ == "__main__":
    unittest.main()
