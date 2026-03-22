from physical_risk_impact.definitions import defs


def test_def_can_load():
    # Check for one of the jobs that actually exists in the definitions
    assert defs.get_job_def("dynamic_sensor_job")
    assert defs.get_job_def("calculate_climate_impact_job")
    assert defs.get_job_def("dynamic_job")
    assert defs.get_job_def("calculate_climate_impact_job")
