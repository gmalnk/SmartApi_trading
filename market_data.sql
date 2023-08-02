CREATE TABLE if not exists dailytf_data (
    id BIGSERIAL PRIMARY KEY,
    index INT NOT NULL,
    token INT NOT NULL,
    time_stamp timestamptz NOT NULL,                                                        
    open_price REAL NOT NULL,
    high_price REAL NOT NULL,
    low_price REAL NOT NULL,
    close_price REAL NOT NULL
);

CREATE TABLE if not exists fifteentf_data (
    id BIGSERIAL PRIMARY KEY,
    index INT NOT NULL,
    token INT NOT NULL,
    time_stamp timestamptz NOT NULL,                                                        
    open_price REAL NOT NULL,
    high_price REAL NOT NULL,
    low_price REAL NOT NULL,
    close_price REAL NOT NULL
);

CREATE TABLE if not exists highlow_data (
    id BIGSERIAL PRIMARY KEY,
    index INT NOT NULL,
    token INT NOT NULL,
    time_stamp timestamptz NOT NULL,                                                         
    open_price REAL NOT NULL,
    high_price REAL NOT NULL,
    low_price REAL NOT NULL,
    close_price REAL NOT NULL,
    high_low VARCHAR(7),
    tf varchar(14)
);

CREATE TABLE if not exists ticks_data (
    id BIGSERIAL PRIMARY KEY,
    symbol_token INT,
    time_stamp timestamptz,                                                        
    ltp REAL
);

CREATE TABLE if not exists trendline_data(
    id BIGSERIAL PRIMARY KEY,
    token INT NOT NULL,
    tf varchar(14),
    slope REAL NOT NULL,
    intercept REAL NOT NULL,
    startdate timestamptz NOT NULL,
    enddate timestamptz NOT NULL,
    hl varchar(2) NOT NULL,
    index1 INT NOT NULL,
    index2 INT NOT NULL,
    index Int Not Null
);

CREATE TABLE if not exists trades_data(
    id BIGSERIAL PRIMARY KEY,
    token INT NOT NULL,
    tf varchar(14) NOT NULL,
    status varchar(3),
    direction varchar(5),
    entry_condition varchar(5),
    entry timestamptz,
    exit timestamptz,
    tp REAL,
    sl REAL,
    bp REAL,
    sp REAL,
    rrr REAL,
    quantity REAL,
    cap REAL,
    current_value Real,
);