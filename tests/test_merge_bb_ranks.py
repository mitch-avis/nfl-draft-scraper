"""Tests for nfl_draft_scraper.merge_bb_ranks_to_picks."""

import polars as pl

from nfl_draft_scraper.merge_bb_ranks_to_picks import (
    _PLAYER_NAME_ALIASES,
    _extract_first_name,
    _fuzzy_match_player,
    _get_av_columns,
    _get_rank_lists,
    _normalize_school,
    _positions_compatible,
    _schools_compatible,
    first_names_compatible,
    last_names_compatible,
)


class TestExtractFirstName:
    """Tests for _extract_first_name."""

    def test_simple_name(self):
        """Verify extracts first name from a simple two-part name."""
        assert _extract_first_name("Patrick Mahomes") == "patrick"

    def test_single_word(self):
        """Verify returns the single word when no space exists."""
        assert _extract_first_name("Cher") == "cher"

    def test_with_suffix(self):
        """Verify first name extracted from name with Jr. suffix."""
        assert _extract_first_name("Kelvin Banks Jr.") == "kelvin"

    def test_empty_string(self):
        """Verify returns empty string for empty input."""
        assert _extract_first_name("") == ""

    def test_case_insensitive(self):
        """Verify result is lowercased."""
        assert _extract_first_name("CAMERON Ward") == "cameron"

    def test_three_part_name(self):
        """Verify extracts first token from multi-part name."""
        assert _extract_first_name("Sean Murphy-Bunting") == "sean"

    def test_apostrophe_in_first_name(self):
        """Verify preserves apostrophes in first name."""
        assert _extract_first_name("Ja'Von Hicks") == "ja'von"

    def test_period_in_first_name(self):
        """Verify preserves periods in first name like D.J."""
        assert _extract_first_name("D.J. Chark") == "d.j."


class TestFirstNamesCompatible:
    """Tests for first_names_compatible."""

    def test_identical_first_names(self):
        """Verify identical first names are compatible."""
        assert first_names_compatible("Patrick Mahomes", "Patrick Mahomes") is True

    def test_abbreviation_pat_patrick(self):
        """Verify Pat is compatible with Patrick (substring)."""
        assert first_names_compatible("Pat Mahomes", "Patrick Mahomes") is True

    def test_abbreviation_cam_cameron(self):
        """Verify Cam is compatible with Cameron (substring)."""
        assert first_names_compatible("Cam Ward", "Cameron Ward") is True

    def test_abbreviation_ben_benjamin(self):
        """Verify Ben is compatible with Benjamin (substring)."""
        assert first_names_compatible("Ben Yurosek", "Benjamin Yurosek") is True

    def test_nickname_bisi_olabisi(self):
        """Verify Bisi is compatible with Olabisi (substring)."""
        assert first_names_compatible("Bisi Johnson", "Olabisi Johnson") is True

    def test_nickname_cobie_decobie(self):
        """Verify Cobie is compatible with Decobie (substring)."""
        assert first_names_compatible("Cobie Durant", "Decobie Durant") is True

    def test_dj_variants(self):
        """Verify DJ and D.J. are compatible (punctuation stripped)."""
        assert first_names_compatible("DJ Chark", "D.J. Chark") is True

    def test_completely_different_first_names(self):
        """Verify Faion and Ja'Von are NOT compatible."""
        assert first_names_compatible("Faion Hicks", "Ja'Von Hicks") is False

    def test_completely_different_lamar_amari(self):
        """Verify Lamar and Amari are NOT compatible."""
        assert first_names_compatible("Lamar Jackson", "Amari Jackson") is False

    def test_completely_different_dee_nate(self):
        """Verify Dee and Nate are NOT compatible."""
        assert first_names_compatible("Dee Wiggins", "Nate Wiggins") is False

    def test_trevon_same_first_name_different_last(self):
        """Verify Trevon matches Trevon even with different full name patterns."""
        assert first_names_compatible("Trevon Moehrig-Woodard", "Trevon Moehrig") is True

    def test_empty_name_a(self):
        """Verify empty name is not compatible."""
        assert first_names_compatible("", "Patrick Mahomes") is False

    def test_empty_name_b(self):
        """Verify empty name is not compatible."""
        assert first_names_compatible("Patrick Mahomes", "") is False

    def test_tre_trequan(self):
        """Verify Tre is compatible with Tre'Quan (substring after punctuation strip)."""
        assert first_names_compatible("Tre Smith", "Tre'Quan Smith") is True

    def test_ugo_ugochukwu(self):
        """Verify Ugo is compatible with Ugochukwu (substring)."""
        assert first_names_compatible("Ugo Amadi", "Ugochukwu Amadi") is True

    def test_rj_variants(self):
        """Verify R.J. and RJ are compatible (punctuation stripped)."""
        assert first_names_compatible("R.J. Harvey Jr.", "RJ Harvey") is True

    def test_bj_variants(self):
        """Verify B.J. and BJ are compatible (punctuation stripped)."""
        assert first_names_compatible("B.J. Green", "BJ Green II") is True

    def test_sean_sean(self):
        """Verify same first name Sean in hyphenated name is compatible."""
        assert first_names_compatible("Sean Murphy-Bunting", "Sean Bunting") is True

    def test_josh_completely_different_jalen(self):
        """Verify Josh and Jalen are NOT compatible."""
        assert first_names_compatible("Josh Allen", "Jalen Allen") is False


class TestLastNamesCompatible:
    """Tests for last_names_compatible."""

    def test_exact_match(self):
        """Verify identical last names are compatible."""
        assert last_names_compatible("john smith", "jane smith") is True

    def test_case_insensitive(self):
        """Verify comparison is case-insensitive."""
        assert last_names_compatible("John Smith", "jane smith") is True

    def test_different_last_names(self):
        """Verify different last names are not compatible."""
        assert last_names_compatible("kobee minor", "kobe king") is False

    def test_suffix_jr_stripped(self):
        """Verify Jr. suffix is stripped before comparison."""
        assert last_names_compatible("andrew booth", "andrew booth jr.") is True

    def test_suffix_iii_stripped(self):
        """Verify III suffix is stripped before comparison."""
        assert last_names_compatible("john metchie", "john metchie iii") is True

    def test_suffix_ii_stripped(self):
        """Verify II suffix is stripped before comparison."""
        assert last_names_compatible("brian asamoah", "brian asamoah ii") is True

    def test_hyphenated_last_name_shares_component(self):
        """Verify hyphenated name matches if a component matches."""
        assert last_names_compatible("tre'vius tomlinson", "tre'vius hodges-tomlinson") is True

    def test_hyphenated_no_shared_component(self):
        """Verify hyphenated name without shared component is not compatible."""
        assert last_names_compatible("julian ashby", "juan davis") is False

    def test_real_bad_match_quan_martin(self):
        """Verify Quan Martin does not match Sean Maginn."""
        assert last_names_compatible("quan martin", "sean maginn") is False

    def test_real_bad_match_caleb_lohner(self):
        """Verify Caleb Lohner does not match Caleb Rogers."""
        assert last_names_compatible("caleb lohner", "caleb rogers") is False

    def test_real_bad_match_junior_bergen(self):
        """Verify Junior Bergen does not match Junior Tafuna."""
        assert last_names_compatible("junior bergen", "junior tafuna") is False

    def test_empty_name(self):
        """Verify empty names are not compatible."""
        assert last_names_compatible("", "alice smith") is False

    def test_single_word_names(self):
        """Verify single-word names are compared directly."""
        assert last_names_compatible("alice", "alice") is True
        assert last_names_compatible("alice", "bob") is False

    def test_nickname_same_last_name(self):
        """Verify nickname with same last name is compatible (Cam → Cameron)."""
        assert last_names_compatible("cam jurgens", "cameron jurgens") is True

    def test_substring_last_name(self):
        """Verify substring-based last name matching (to'oto'o ↔ to'o-to'o)."""
        assert last_names_compatible("henry to'oto'o", "henry to'o-to'o") is True


class TestPositionsCompatible:
    """Tests for _positions_compatible."""

    def test_same_position(self):
        """Verify identical positions are compatible."""
        assert _positions_compatible("CB", "CB") is True

    def test_case_insensitive(self):
        """Verify comparison is case-insensitive."""
        assert _positions_compatible("cb", "CB") is True

    def test_edge_vs_de(self):
        """Verify EDGE matches DE (both pass rushers)."""
        assert _positions_compatible("EDGE", "DE") is True

    def test_ed_vs_de(self):
        """Verify ED (JLBB code) matches DE."""
        assert _positions_compatible("ED", "DE") is True

    def test_edge_vs_olb(self):
        """Verify EDGE matches OLB (edge rusher listed as linebacker)."""
        assert _positions_compatible("EDGE", "OLB") is True

    def test_dl_vs_dt(self):
        """Verify DL matches DT (general vs specific defensive line)."""
        assert _positions_compatible("DL", "DT") is True

    def test_dl_vs_de(self):
        """Verify DL matches DE."""
        assert _positions_compatible("DL", "DE") is True

    def test_idl_vs_dt(self):
        """Verify IDL (interior DL) matches DT."""
        assert _positions_compatible("IDL", "DT") is True

    def test_iol_vs_g(self):
        """Verify IOL matches G (interior OL vs guard)."""
        assert _positions_compatible("IOL", "G") is True

    def test_iol_vs_c(self):
        """Verify IOL matches C (interior OL vs center)."""
        assert _positions_compatible("IOL", "C") is True

    def test_ot_vs_t(self):
        """Verify OT matches T (offensive tackle variants)."""
        assert _positions_compatible("OT", "T") is True

    def test_cb_vs_s_compatible_as_db(self):
        """Verify CB and S are compatible (both defensive backs)."""
        assert _positions_compatible("CB", "S") is True

    def test_s_vs_fs(self):
        """Verify S matches FS (safety variants)."""
        assert _positions_compatible("S", "FS") is True

    def test_saf_vs_s(self):
        """Verify SAF matches S."""
        assert _positions_compatible("SAF", "S") is True

    def test_lb_vs_edge(self):
        """Verify generic LB matches EDGE (some LBs are edge players)."""
        assert _positions_compatible("LB", "EDGE") is True

    def test_ilb_vs_edge_incompatible(self):
        """Verify ILB does not match EDGE (pure inside vs edge)."""
        assert _positions_compatible("ILB", "EDGE") is False

    def test_cb_vs_dl_incompatible(self):
        """Verify CB does not match DL (defensive back vs defensive line)."""
        assert _positions_compatible("CB", "DL") is False

    def test_qb_vs_wr_incompatible(self):
        """Verify QB does not match WR."""
        assert _positions_compatible("QB", "WR") is False

    def test_k_vs_p_incompatible(self):
        """Verify K does not match P."""
        assert _positions_compatible("K", "P") is False

    def test_empty_position_compatible(self):
        """Verify empty position is treated as compatible (unknown)."""
        assert _positions_compatible("", "CB") is True

    def test_unknown_position_compatible(self):
        """Verify unknown position code is treated as compatible."""
        assert _positions_compatible("XYZ", "CB") is True

    def test_rb_vs_fb(self):
        """Verify RB matches FB (same backfield group)."""
        assert _positions_compatible("RB", "FB") is True

    def test_cbn_vs_cb(self):
        """Verify CBN (nickel corner) matches CB."""
        assert _positions_compatible("CBN", "CB") is True

    def test_ol_vs_ot(self):
        """Verify generic OL matches OT."""
        assert _positions_compatible("OL", "OT") is True

    def test_db_vs_cb(self):
        """Verify generic DB matches CB."""
        assert _positions_compatible("DB", "CB") is True

    def test_nt_vs_dl(self):
        """Verify NT (nose tackle) matches DL."""
        assert _positions_compatible("NT", "DL") is True


class TestNormalizeSchool:
    """Tests for _normalize_school."""

    def test_already_canonical(self):
        """Verify canonical name passes through lowercased."""
        assert _normalize_school("Alabama") == "alabama"

    def test_state_abbreviation(self):
        """Verify 'St.' is expanded to 'state'."""
        assert _normalize_school("Penn St.") == "penn state"

    def test_college_abbreviation(self):
        """Verify 'Col.' is expanded to 'college'."""
        assert _normalize_school("Boston Col.") == "boston college"

    def test_case_insensitive(self):
        """Verify normalization is case-insensitive."""
        assert _normalize_school("PENN STATE") == "penn state"

    def test_strips_whitespace(self):
        """Verify leading/trailing whitespace is stripped."""
        assert _normalize_school("  Alabama  ") == "alabama"

    def test_ucf_alias(self):
        """Verify UCF maps to 'central florida'."""
        assert _normalize_school("UCF") == "central florida"

    def test_central_florida_alias(self):
        """Verify 'Central Florida' also maps to 'central florida'."""
        assert _normalize_school("Central Florida") == "central florida"

    def test_utsa_alias(self):
        """Verify UTSA maps to canonical form."""
        assert _normalize_school("UTSA") == "utsa"

    def test_texas_san_antonio_alias(self):
        """Verify 'Texas-San Antonio' maps to same form as UTSA."""
        assert _normalize_school("Texas-San Antonio") == "utsa"

    def test_nc_state_alias(self):
        """Verify 'NC State' maps to 'north carolina state'."""
        assert _normalize_school("NC State") == "north carolina state"

    def test_north_carolina_st_normalizes(self):
        """Verify 'North Carolina St.' normalizes to same as NC State."""
        assert _normalize_school("North Carolina St.") == "north carolina state"

    def test_full_state_name_unchanged(self):
        """Verify 'Penn State' stays as 'penn state'."""
        assert _normalize_school("Penn State") == "penn state"

    def test_florida_st_expanded(self):
        """Verify 'Florida St.' expanded to 'florida state'."""
        assert _normalize_school("Florida St.") == "florida state"

    def test_middle_tenn_st_alias(self):
        """Verify 'Middle Tenn. St.' resolves via alias."""
        assert _normalize_school("Middle Tenn. St.") == "middle tennessee state"

    def test_se_missouri_alias(self):
        """Verify 'SE Missouri St.' resolves via alias."""
        assert _normalize_school("SE Missouri St.") == "southeast missouri state"

    def test_nw_missouri_alias(self):
        """Verify 'NW Missouri St.' resolves via alias."""
        assert _normalize_school("NW Missouri St.") == "northwest missouri state"

    def test_houston_christian_alias(self):
        """Verify 'Houston Baptist' maps to 'houston christian'."""
        assert _normalize_school("Houston Baptist") == "houston christian"

    def test_ala_birmingham_alias(self):
        """Verify 'Ala-Birmingham' maps to same as UAB."""
        assert _normalize_school("Ala-Birmingham") == _normalize_school("UAB")

    def test_miami_fl_preserved(self):
        """Verify 'Miami (FL)' is preserved distinctly from 'Miami (OH)'."""
        assert _normalize_school("Miami (FL)") != _normalize_school("Miami (OH)")


class TestSchoolsCompatible:
    """Tests for _schools_compatible."""

    def test_same_school(self):
        """Verify identical school names are compatible."""
        assert _schools_compatible("Alabama", "Alabama") is True

    def test_penn_state_vs_penn_st(self):
        """Verify 'Penn State' matches 'Penn St.'."""
        assert _schools_compatible("Penn State", "Penn St.") is True

    def test_empty_school_compatible(self):
        """Verify empty school is treated as compatible (unknown)."""
        assert _schools_compatible("", "Alabama") is True

    def test_ucf_vs_central_florida(self):
        """Verify UCF matches 'Central Florida'."""
        assert _schools_compatible("UCF", "Central Florida") is True

    def test_different_schools(self):
        """Verify different schools are not compatible."""
        assert _schools_compatible("Alabama", "Nebraska") is False

    def test_case_insensitive(self):
        """Verify comparison is case-insensitive."""
        assert _schools_compatible("ALABAMA", "alabama") is True

    def test_nc_state_vs_north_carolina_st(self):
        """Verify 'NC State' matches 'North Carolina St.'."""
        assert _schools_compatible("NC State", "North Carolina St.") is True

    def test_utsa_vs_texas_san_antonio(self):
        """Verify UTSA matches 'Texas-San Antonio'."""
        assert _schools_compatible("UTSA", "Texas-San Antonio") is True

    def test_tulane_vs_nebraska(self):
        """Verify Tulane does not match Nebraska (Micah Robinson case)."""
        assert _schools_compatible("Tulane", "Nebraska") is False


class TestFuzzyMatchPlayer:
    """Tests for _fuzzy_match_player."""

    def test_exact_match(self):
        """Verify exact match."""
        assert _fuzzy_match_player("Alice", ["Alice", "Bob"]) == "Alice"

    def test_fuzzy_match(self):
        """Verify fuzzy match with close first name and same last name."""
        result = _fuzzy_match_player("cam jurgens", ["cameron jurgens", "bob jones"])
        assert result == "cameron jurgens"

    def test_no_match(self):
        """Verify no match."""
        assert _fuzzy_match_player("ZZZZZ", ["Alice", "Bob"]) is None

    def test_empty_choices(self):
        """Verify empty choices."""
        assert _fuzzy_match_player("Alice", []) is None

    def test_custom_cutoff(self):
        """Verify custom cutoff."""
        assert _fuzzy_match_player("A", ["Alice"], cutoff=0.99) is None

    def test_rejects_different_last_name(self):
        """Verify fuzzy match is rejected when last names differ."""
        # "kobee minor" would fuzzy-match "kobe king" without the last-name check
        assert _fuzzy_match_player("kobee minor", ["kobe king"]) is None

    def test_rejects_quan_martin_to_sean_maginn(self):
        """Verify Quan Martin is not matched to Sean Maginn."""
        assert _fuzzy_match_player("quan martin", ["sean maginn"]) is None

    def test_allows_nickname_same_last_name(self):
        """Verify nickname + same last name still matches (Tank Dell → nathaniel dell)."""
        result = _fuzzy_match_player("tank dell", ["nathaniel dell"])
        assert result == "nathaniel dell"

    def test_allows_suffix_variation(self):
        """Verify suffix variations still match (Will Anderson → will anderson jr.)."""
        result = _fuzzy_match_player("will anderson", ["will anderson jr."])
        assert result == "will anderson jr."

    def test_rejects_when_position_and_school_both_disagree(self):
        """Verify match is rejected when both position and school are incompatible."""
        # Micah Robinson (CB, Tulane) should not match Ty Robinson (DL, Nebraska)
        result = _fuzzy_match_player(
            "micah robinson",
            ["ty robinson"],
            pick_position="CB",
            pick_school="Tulane",
            bb_positions={"ty robinson": "DL"},
            bb_schools={"ty robinson": "Nebraska"},
        )
        assert result is None

    def test_allows_when_position_disagrees_but_school_agrees(self):
        """Verify match is kept when position differs but school matches."""
        result = _fuzzy_match_player(
            "cam smith",
            ["cameron smith"],
            pick_position="CB",
            pick_school="Alabama",
            bb_positions={"cameron smith": "S"},
            bb_schools={"cameron smith": "Alabama"},
        )
        assert result == "cameron smith"

    def test_allows_when_school_disagrees_but_position_agrees(self):
        """Verify match is kept when school differs but position matches (transfer)."""
        result = _fuzzy_match_player(
            "cam smith",
            ["cameron smith"],
            pick_position="CB",
            pick_school="Georgia",
            bb_positions={"cameron smith": "CB"},
            bb_schools={"cameron smith": "Alabama"},
        )
        assert result == "cameron smith"

    def test_allows_when_no_position_school_data(self):
        """Verify match is kept when position/school data is not provided."""
        result = _fuzzy_match_player("cam jurgens", ["cameron jurgens"])
        assert result == "cameron jurgens"

    def test_allows_compatible_position_and_school(self):
        """Verify match is kept when both position and school are compatible."""
        result = _fuzzy_match_player(
            "will anderson",
            ["will anderson jr."],
            pick_position="DE",
            pick_school="Alabama",
            bb_positions={"will anderson jr.": "EDGE"},
            bb_schools={"will anderson jr.": "Alabama"},
        )
        assert result == "will anderson jr."


class TestGetAvColumns:
    """Tests for _get_av_columns."""

    def test_returns_matching_columns(self):
        """Verify returns matching columns."""
        df = pl.DataFrame(
            {"2020": [1], "2021": [2], "career": [3], "weighted_career": [4], "other": [5]}
        )
        result = _get_av_columns(df)
        assert "2020" in result
        assert "2021" in result
        assert "career" in result
        assert "weighted_career" in result
        assert "other" not in result

    def test_returns_empty_for_no_matches(self):
        """Verify returns empty for no matches."""
        df = pl.DataFrame({"col_a": [1], "col_b": [2]})
        result = _get_av_columns(df)
        assert result == []


class TestGetRankLists:
    """Tests for _get_rank_lists."""

    def test_matches_player(self):
        """Verify matches player."""
        picks = pl.DataFrame(
            {
                "pfr_player_name": ["Alice"],
                "pfr_player_name_clean": ["alice"],
                "position": ["QB"],
                "college": ["MIT"],
            }
        )
        bb_lookup = {
            "alice": {
                "MDDB": 1,
                "JLBB": 3,
                "Consensus": 2.0,
                "JL_Avg": 4.0,
                "JL_SD": 1.0,
                "JL_Sources": 3,
                "Sources": 9,
            }
        }
        bb_positions = {"alice": "QB"}
        bb_schools = {"alice": "MIT"}
        result = _get_rank_lists(picks, bb_lookup, ["alice"], bb_positions, bb_schools)
        assert result["MDDB Rank"] == [1]
        assert result["JLBB Rank"] == [3]
        assert result["Consensus"] == [2.0]
        assert result["JL_Avg"] == [4.0]
        assert result["JL_SD"] == [1.0]
        assert result["JL_Sources"] == [3]
        assert result["Sources"] == [9]

    def test_no_match(self):
        """Verify no match."""
        picks = pl.DataFrame(
            {
                "pfr_player_name": ["Zzzz"],
                "pfr_player_name_clean": ["zzzz"],
                "position": ["QB"],
                "college": ["Nowhere"],
            }
        )
        bb_lookup = {
            "alice": {
                "MDDB": 1,
                "JLBB": 3,
                "Consensus": 2.0,
                "JL_Avg": 4.0,
                "JL_SD": 1.0,
                "JL_Sources": 3,
                "Sources": 9,
            }
        }
        bb_positions = {"alice": "QB"}
        bb_schools = {"alice": "MIT"}
        result = _get_rank_lists(picks, bb_lookup, ["alice"], bb_positions, bb_schools)
        assert result["MDDB Rank"] == [None]
        assert result["JLBB Rank"] == [None]
        assert result["Consensus"] == [None]
        assert result["JL_Avg"] == [None]
        assert result["JL_SD"] == [None]
        assert result["JL_Sources"] == [None]
        assert result["Sources"] == [None]

    def test_multiple_players(self):
        """Verify multiple players."""
        picks = pl.DataFrame(
            {
                "pfr_player_name": ["Alice", "Bob"],
                "pfr_player_name_clean": ["alice", "bob"],
                "position": ["QB", "RB"],
                "college": ["MIT", "MIT"],
            }
        )
        bb_lookup = {
            "alice": {
                "MDDB": 1,
                "JLBB": 3,
                "Consensus": 2.0,
                "JL_Avg": 4.0,
                "JL_SD": 1.0,
                "JL_Sources": 3,
                "Sources": 9,
            },
            "bob": {
                "MDDB": 5,
                "JLBB": 7,
                "Consensus": 6.0,
                "JL_Avg": 7.0,
                "JL_SD": 2.0,
                "JL_Sources": 5,
                "Sources": 11,
            },
        }
        bb_positions = {"alice": "QB", "bob": "RB"}
        bb_schools = {"alice": "MIT", "bob": "MIT"}
        result = _get_rank_lists(picks, bb_lookup, ["alice", "bob"], bb_positions, bb_schools)
        assert result["MDDB Rank"] == [1, 5]
        assert result["JLBB Rank"] == [3, 7]
        assert result["Consensus"] == [2.0, 6.0]

    def test_rejects_match_with_incompatible_position_and_school(self):
        """Verify fuzzy match is rejected when both position and school disagree."""
        picks = pl.DataFrame(
            {
                "pfr_player_name": ["Micah Robinson"],
                "pfr_player_name_clean": ["micah robinson"],
                "position": ["CB"],
                "college": ["Tulane"],
            }
        )
        bb_lookup = {
            "ty robinson": {
                "MDDB": 93,
                "JLBB": 107,
                "Consensus": 100.0,
                "JL_Avg": 107.0,
                "JL_SD": 5.0,
                "JL_Sources": 10,
                "Sources": 16,
            }
        }
        bb_positions = {"ty robinson": "DL"}
        bb_schools = {"ty robinson": "Nebraska"}
        result = _get_rank_lists(picks, bb_lookup, ["ty robinson"], bb_positions, bb_schools)
        assert result["MDDB Rank"] == [None]
        assert result["JLBB Rank"] == [None]
        assert result["Consensus"] == [None]

    def test_uses_alias_for_boogie_basham(self):
        """Verify Boogie Basham matches Carlos Basham Jr. via alias lookup."""
        picks = pl.DataFrame(
            {
                "pfr_player_name": ["Boogie Basham"],
                "pfr_player_name_clean": ["boogie basham"],
                "position": ["DL"],
                "college": ["Wake Forest"],
            }
        )
        bb_lookup = {
            "carlos basham jr.": {
                "MDDB": 48,
                "JLBB": 40,
                "Consensus": 46.5,
                "JL_Avg": 45.9,
                "JL_SD": 11.5,
                "JL_Sources": 16,
                "Sources": 22,
            }
        }
        bb_positions = {"carlos basham jr.": "EDGE"}
        bb_schools = {"carlos basham jr.": "Wake Forest"}
        result = _get_rank_lists(picks, bb_lookup, ["carlos basham jr."], bb_positions, bb_schools)
        assert result["MDDB Rank"] == [48]
        assert result["JLBB Rank"] == [40]
        assert result["Consensus"] == [46.5]

    def test_uses_alias_for_quan_martin(self):
        """Verify Quan Martin matches Jartavius Martin via alias lookup."""
        picks = pl.DataFrame(
            {
                "pfr_player_name": ["Quan Martin"],
                "pfr_player_name_clean": ["quan martin"],
                "position": ["DB"],
                "college": ["Illinois"],
            }
        )
        bb_lookup = {
            "jartavius martin": {
                "MDDB": 84,
                "JLBB": 74,
                "Consensus": 85.1,
                "JL_Avg": 85.5,
                "JL_SD": 28.0,
                "JL_Sources": 17,
                "Sources": 23,
            }
        }
        bb_positions = {"jartavius martin": "S"}
        bb_schools = {"jartavius martin": "Illinois"}
        result = _get_rank_lists(picks, bb_lookup, ["jartavius martin"], bb_positions, bb_schools)
        assert result["MDDB Rank"] == [84]
        assert result["JLBB Rank"] == [74]
        assert result["Consensus"] == [85.1]


class TestPlayerNameAliases:
    """Tests for _PLAYER_NAME_ALIASES mapping."""

    def test_boogie_basham_alias(self):
        """Verify Boogie Basham maps to Carlos Basham Jr."""
        assert _PLAYER_NAME_ALIASES["boogie basham"] == "carlos basham jr."

    def test_quan_martin_alias(self):
        """Verify Quan Martin maps to Jartavius Martin."""
        assert _PLAYER_NAME_ALIASES["quan martin"] == "jartavius martin"

    def test_kobee_minor_alias(self):
        """Verify Kobee Minor maps to Darrian Minor."""
        assert _PLAYER_NAME_ALIASES["kobee minor"] == "darrian minor"

    def test_julian_ashby_alias(self):
        """Verify Julian Ashby maps to Frederick Julian Ashby."""
        assert "julian ashby" in _PLAYER_NAME_ALIASES

    def test_alias_keys_are_lowercase(self):
        """Verify all alias keys are lowercase."""
        for key in _PLAYER_NAME_ALIASES:
            assert key == key.lower(), f"Alias key '{key}' is not lowercase"

    def test_alias_values_are_lowercase(self):
        """Verify all alias values are lowercase."""
        for value in _PLAYER_NAME_ALIASES.values():
            assert value == value.lower(), f"Alias value '{value}' is not lowercase"
