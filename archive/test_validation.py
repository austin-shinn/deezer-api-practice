"""Unit tests for the row-count and sanity checks that run before each upload.

Run: python -m unittest -v test_validation
"""
import unittest

import deezer_pull
import lastfm_pull
from lake import DataQualityError, raise_if_failed

PER_GENRE = 25
GENRE_COUNT = 27
TAGS = [f"tag{i}" for i in range(25)]
PER_TAG = 25
CHART_SIZE = 100


def deezer_snapshot() -> dict[str, list[dict]]:
    genres = [{"genre_id": g, "genre_name": f"genre{g}"} for g in range(GENRE_COUNT)]
    chart = [
        {"genre_id": g, "position": p, "artist_id": g * 100 + p}
        for g in range(GENRE_COUNT)
        for p in range(1, PER_GENRE + 1)
    ]
    artists = [{"artist_id": a, "nb_fan": 1000} for a in sorted({row["artist_id"] for row in chart})]
    return {"genres": genres, "genre_chart_tracks": chart, "artists": artists}


def lastfm_snapshot() -> dict[str, list[dict]]:
    tag_rows = [{"tag": t, "rank": r} for t in TAGS for r in range(1, PER_TAG + 1)]
    chart_rows = [{"rank": r} for r in range(1, CHART_SIZE + 1)]
    artists = [{"artist_name": f"artist{i}", "listeners": 500} for i in range(700)]
    return {"tag_top_artists": tag_rows, "chart_top_artists": chart_rows, "artists": artists}


class DeezerValidationTest(unittest.TestCase):
    def check(self, snapshot):
        return deezer_pull.validate(snapshot, PER_GENRE)

    def test_complete_snapshot_passes(self):
        self.assertEqual(self.check(deezer_snapshot()), [])

    def test_slightly_short_chart_still_passes(self):
        snapshot = deezer_snapshot()
        # Drop one track from each of the first three genres (well inside the 10 percent tolerance).
        for genre_id in range(3):
            row = next(r for r in snapshot["genre_chart_tracks"] if r["genre_id"] == genre_id)
            snapshot["genre_chart_tracks"].remove(row)
        remaining = {r["artist_id"] for r in snapshot["genre_chart_tracks"]}
        snapshot["artists"] = [a for a in snapshot["artists"] if a["artist_id"] in remaining]
        self.assertEqual(self.check(snapshot), [])

    def test_half_the_chart_fails(self):
        snapshot = deezer_snapshot()
        snapshot["genre_chart_tracks"] = snapshot["genre_chart_tracks"][::2]
        problems = self.check(snapshot)
        self.assertTrue(any("genre_chart_tracks" in p for p in problems))

    def test_too_few_genres_fails(self):
        snapshot = deezer_snapshot()
        snapshot["genres"] = snapshot["genres"][:5]
        self.assertTrue(any("genres:" in p for p in self.check(snapshot)))

    def test_genre_without_chart_rows_fails(self):
        snapshot = deezer_snapshot()
        snapshot["genre_chart_tracks"] = [r for r in snapshot["genre_chart_tracks"] if r["genre_id"] != 4]
        problems = self.check(snapshot)
        self.assertTrue(any("no chart rows" in p for p in problems))

    def test_missing_fan_counts_fail(self):
        snapshot = deezer_snapshot()
        for artist in snapshot["artists"][:100]:
            artist["nb_fan"] = None
        self.assertTrue(any("nb_fan" in p for p in self.check(snapshot)))

    def test_artist_count_mismatch_fails(self):
        snapshot = deezer_snapshot()
        snapshot["artists"] = snapshot["artists"][:-10]
        self.assertTrue(any("artists:" in p for p in self.check(snapshot)))


class LastfmValidationTest(unittest.TestCase):
    def check(self, snapshot, extra_count=400):
        return lastfm_pull.validate(snapshot, TAGS, PER_TAG, CHART_SIZE, extra_count)

    def test_complete_snapshot_passes(self):
        self.assertEqual(self.check(lastfm_snapshot()), [])

    def test_short_tag_charts_fail(self):
        snapshot = lastfm_snapshot()
        snapshot["tag_top_artists"] = snapshot["tag_top_artists"][:300]
        self.assertTrue(any("tag_top_artists" in p for p in self.check(snapshot)))

    def test_tag_with_no_artists_fails(self):
        snapshot = lastfm_snapshot()
        snapshot["tag_top_artists"] = [r for r in snapshot["tag_top_artists"] if r["tag"] != "tag3"]
        self.assertTrue(any("tags with no artists" in p for p in self.check(snapshot)))

    def test_short_global_chart_fails(self):
        snapshot = lastfm_snapshot()
        snapshot["chart_top_artists"] = snapshot["chart_top_artists"][:50]
        self.assertTrue(any("chart_top_artists" in p for p in self.check(snapshot)))

    def test_missing_cross_referenced_artists_fail(self):
        snapshot = lastfm_snapshot()
        snapshot["artists"] = snapshot["artists"][:200]
        self.assertTrue(any("cross-referenced" in p for p in self.check(snapshot, extra_count=400)))

    def test_null_listener_counts_fail(self):
        snapshot = lastfm_snapshot()
        for artist in snapshot["artists"][:100]:
            artist["listeners"] = None
        self.assertTrue(any("listeners" in p for p in self.check(snapshot)))


class RaiseIfFailedTest(unittest.TestCase):
    def test_empty_list_does_not_raise(self):
        raise_if_failed([])

    def test_problems_raise_with_all_messages(self):
        with self.assertRaises(DataQualityError) as context:
            raise_if_failed(["first problem", "second problem"])
        self.assertIn("first problem", str(context.exception))
        self.assertIn("second problem", str(context.exception))


if __name__ == "__main__":
    unittest.main()
