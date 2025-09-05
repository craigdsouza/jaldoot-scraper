from dataclasses import dataclass


@dataclass
class SeasonDef:
    url: str
    table_id: str
    state_column_map: dict
    drop_total_row: bool = True


SEASONS = {
    # Post Monsoon 2022
    "post-monsoon-2022": SeasonDef(
        url="https://mnregaweb4.nic.in/Jaldootweb/ReportABA/WaterCoveredReport.aspx",
        table_id="ContentPlaceHolder1_gvReport",
        state_column_map={
            "States/UT's": "state_ut",
            "Total No. of Panchayat": "total_no_of_panchayat",
            "No. of Panchayat Covered": "no_of_panchayat_covered",
            "No. of Village Covered": "no_of_village_covered",
            "No. of Well Covered": "no_of_well_covered",
            "0-2 Feet": "no_of_wells_0_2_feet",
            "3-5 Feet": "no_of_wells_3_5_feet",
            "6-10 Feet": "no_of_wells_6_10_feet",
            ">10 Feet": "no_of_wells_gt_10_feet",
            "URL": "url",
        },
    ),
    # Pre Monsoon 2023
    "pre-monsoon-2023": SeasonDef(
        url="https://mnregaweb4.nic.in/Jaldootweb/ReportABA/WaterLevelReport2023.aspx",
        table_id="ContentPlaceHolder1_gvReport",
        state_column_map={
            "States/UT's": "state_ut",
            "Total No. of Panchayat": "total_no_of_panchayat",
            "No. of Panchayat Covered": "no_of_panchayat_covered",
            "No. of Village Covered": "no_of_village_covered",
            "No. of Well Covered": "no_of_well_covered",
            # Depth bands may be absent for this season; optional mapping retained
            "0-2 Feet": "no_of_wells_0_2_feet",
            "3-5 Feet": "no_of_wells_3_5_feet",
            "6-10 Feet": "no_of_wells_6_10_feet",
            ">10 Feet": "no_of_wells_gt_10_feet",
            "URL": "url",
        },
    ),
    # Post Monsoon 2023
    "post-monsoon-2023": SeasonDef(
        url="https://mnregaweb4.nic.in/Jaldootweb/ReportABA/WellCoveredReport.aspx",
        table_id="ContentPlaceHolder1_gvReport",
        state_column_map={
            "States/UT's": "state_ut",
            "Total No. of Panchayat": "total_no_of_panchayat",
            "No. of Panchayat Covered": "no_of_panchayat_covered",
            "No. of Village Covered": "no_of_village_covered",
            "No. of Well Covered": "no_of_well_covered",
            "0-2 Feet": "no_of_wells_0_2_feet",
            "3-5 Feet": "no_of_wells_3_5_feet",
            "6-10 Feet": "no_of_wells_6_10_feet",
            ">10 Feet": "no_of_wells_gt_10_feet",
            "URL": "url",
        },
    ),
    # Pre Monsoon 2024
    "pre-monsoon-2024": SeasonDef(
        url="https://mnregaweb4.nic.in/Jaldootweb/ReportABA/WellCoveredReportPre_2024.aspx",
        table_id="ContentPlaceHolder1_gvReport",
        state_column_map={
            "States/UT's": "state_ut",
            "Total No. of Panchayat": "total_no_of_panchayat",
            "No. of Panchayat Covered": "no_of_panchayat_covered",
            "No. of Village Covered": "no_of_village_covered",
            "No. of Well Covered": "no_of_well_covered",
            "0-2 Feet": "no_of_wells_0_2_feet",
            "3-5 Feet": "no_of_wells_3_5_feet",
            "6-10 Feet": "no_of_wells_6_10_feet",
            ">10 Feet": "no_of_wells_gt_10_feet",
            "URL": "url",
        },
    ),
    # Post Monsoon 2024
    "post-monsoon-2024": SeasonDef(
        url="https://mnregaweb4.nic.in/Jaldootweb/ReportABA/WellCoveredReportPost_2024.aspx",
        table_id="ContentPlaceHolder1_gvReport",
        state_column_map={
            "States/UT's": "state_ut",
            "Total No. of Panchayat": "total_no_of_panchayat",
            "No. of Panchayat Covered": "no_of_panchayat_covered",
            "No. of Village Covered": "no_of_village_covered",
            "No. of Well Covered": "no_of_well_covered",
            "0-2 Feet": "no_of_wells_0_2_feet",
            "3-5 Feet": "no_of_wells_3_5_feet",
            "6-10 Feet": "no_of_wells_6_10_feet",
            ">10 Feet": "no_of_wells_gt_10_feet",
            "URL": "url",
        },
    ),
    # Pre Monsoon 2025
    "pre-monsoon-2025": SeasonDef(
        url="https://mnregaweb4.nic.in/Jaldootweb/ReportABA/WellCoveredReportPre_2025.aspx",
        table_id="ContentPlaceHolder1_gvReport",
        state_column_map={
            "States/UT's": "state_ut",
            "Total No. of Panchayat": "total_no_of_panchayat",
            "No. of Panchayat Covered": "no_of_panchayat_covered",
            "No. of Village Covered": "no_of_village_covered",
            "No. of Well Covered": "no_of_well_covered",
            "0-2 Feet": "no_of_wells_0_2_feet",
            "3-5 Feet": "no_of_wells_3_5_feet",
            "6-10 Feet": "no_of_wells_6_10_feet",
            ">10 Feet": "no_of_wells_gt_10_feet",
            "URL": "url",
        },
    ),
    # Extend with 2024/2025 keys as needed using the same pattern
}

__all__ = ["SeasonDef", "SEASONS"]


