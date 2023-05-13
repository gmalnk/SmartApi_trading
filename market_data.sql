CREATE TABLE dailytf_data (
    id BIGSERIAL PRIMARY KEY,
    index INT NOT NULL,
    token INT NOT NULL,
    time_stamp timestamptz NOT NULL,                                                        
    open_price REAL NOT NULL,
    high_price REAL NOT NULL,
    low_price REAL NOT NULL,
    close_price REAL NOT NULL
);

CREATE TABLE fifteentf_data (
    id BIGSERIAL PRIMARY KEY,
    index INT NOT NULL,
    token INT NOT NULL,
    time_stamp timestamptz NOT NULL,                                                        
    open_price REAL NOT NULL,
    high_price REAL NOT NULL,
    low_price REAL NOT NULL,
    close_price REAL NOT NULL
);

CREATE TABLE highlow_data (
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
)

create table ticks_data (
    id BIGSERIAL PRIMARY KEY,
    symbol_token INT,
    time_stamp timestamptz,                                                        
    ltp REAL
);



CREATE TABLE trendline_data(
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
)