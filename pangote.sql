--
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
CREATE TABLE IF NOT EXISTS meas(
    id                  SERIAL PRIMARY KEY,
    deveui              INT NOT NULL,
    t                   TIMESTAMP NOT NULL,
    pos                 POINT NOT NULL,
    accuracy            INT NOT NULL,
    batt_V              FLOAT,
    batt_cap            INT,
    temp                INT,
    rssi                FLOAT,
    snr                 FLOAT
);

-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
CREATE TABLE IF NOT EXISTS trackers(
    devui               INT NOT NULL PRIMARY KEY,
    label               VARCHAR NOT NULL
);

-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
CREATE TABLE IF NOT EXISTS cows(
    label               VARCHAR PRIMARY KEY,
    name                VARCHAR NOT NULL UNIQUE,
    birthday            TIMESTAMP
);





