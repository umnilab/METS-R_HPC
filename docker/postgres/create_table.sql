CREATE TABLE bsm (
    ID SERIAL PRIMARY KEY,
    VID INTEGER,
    UTC_FIX_MODE INTEGER,
    LATITUDE NUMERIC,
    LONGITUDE NUMERIC,
    ALTITUDE NUMERIC,
    QTY_SV_IN_VIEW INTEGER,
    QTY_SV_USED INTEGER,
    GNSS_STATUS_UNAVAILABLE BOOLEAN,
    GNSS_STATUS_APDOPOFUNDER5 BOOLEAN,
    GNSS_STATUS_INVIEWOFUNDER5 BOOLEAN,
    GNSS_STATUS_LOCALCORRECTIONSPRESENT BOOLEAN,
    GNSS_STATUS_NETWORKCORRECTIONSPRESENT BOOLEAN,
    SEMIMAJORAXISACCURACY NUMERIC,
    SEMIMINORAXISACCURACY NUMERIC,
    SEMIMAJORAXISORIENTATION NUMERIC,
    HEADING NUMERIC,
    VELOCITY NUMERIC,
    CLIMB NUMERIC,
    TIME_CONFIDENCE NUMERIC,
    VELOCITY_CONFIDENCE NUMERIC,
    ELEVATION_CONFIDENCE NUMERIC,
    LEAP_SECONDS INTEGER,
    UTC_TIME NUMERIC
);