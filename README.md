![The Fool](img/thefool.jpg?raw=true "The Fool")

# cryptomancer

cryptomancer is a Python library for automating cryto trades.

## Disclosures

                   USE AT YOUR OWN RISK

                     .ed"""" """$$$$be.
                   -"           ^""**$$$e.
                 ."                   '$$$c
                /                      "4$$b
               d  3                      $$$$
               $  *                   .$$$$$$
              .$  ^c           $$$$$e$$$$$$$$.
              d$L  4.         4$$$$$$$$$$$$$$b
              $$$$b ^ceeeee.  4$$ECL.F*$$$$$$$
  e$""=.      $$$$P d$$$$F $ $$$$$$$$$- $$$$$$
 z$$b. ^c     3$$$F "$$$$b   $"$$$$$$$  $$$$*"      .=""$c
4$$$$L        $$P"  "$$b   .$ $$$$$...e$$        .=  e$$$.
^*$$$$$c  %..   *c    ..    $$ 3$$$$$$$$$$eF     zP  d$$$$$
  "**$$$ec   "   %ce""    $$$  $$$$$$$$$$*    .r" =$$$$P""
        "*$b.  "c  *$e.    $$$ d$$$$$"L$$    .d"  e$$***"
          ^*$$c ^$c $$$      4J$$$$$% $$$ .e*".eeP"
             "$$$$$$"'$=e....$*$$**$cz$$" "..d$*"
               "*$$$  *=%4.$ L L$ P3$$$F $$$P"
                  "$   "%*ebJLzb$e$$$$$b $P"
                    %..      4$$$$$$$$$$ "
                     $$$e   z$$$$$$$$$$%
                      "*$c  "$$$$$$$P"
                       ."""*$$$$$$$$bc
                    .-"    .$***$$$"""*e.
                 .-"    .e$"     "*$c  ^*b.
          .=*""""    .e$*"          "*bc  "*$e..
        .$"        .z*"               ^*$e.   "*****e.
        $$ee$c   .d"                     "*$.        3.
        ^*$E")$..$"                         *   .ee==d%
           $.d$$$*                           *  J$$$e*
            """""                              "$$$"

## Installation

You'll first need to install the `cryptomancer` library.

```bash
>>> git clone git@github.com:choffstein/cryptomancer.git
>>> cd cryptomancer/
>>> python setup.py install
```

The `requirements.txt` file has all the necessary libraries.  

Note that a [pull request](https://github.com/quan-digital/ftx/pull/15) has been submitted to the `ftx` library.  If it has not been accepted, a fork of the library can found [here](https://github.com/choffstein/ftx/tree/order_status).

## Secrets
Authentication information for different services is expected to be found in a `.secrets/` found in the same root folder where the python scripts are being run. 

## Setting up the Database
For code relying upon the `cryptomancer.security_master` module, a postgresql server needs to be created.  Corresponding authentication information should be kept in `.secrets/postgres.json` in the format:

```json
{
    "SQL_URL":  "url.com",
    "SQL_PORT": "5432",
    "SQL_DB": "db_name",
    "SQL_USER": "db_user",
    "SQL_PASSWORD": "db_password"
}
```

Once the server has been set up, you'll want to use `alembic` to create the tables.  Make sure to edit the `alembic.ini` file to change the `sqlalchemy.url` configuration:

```
sqlalchemy.url = postgresql://user:password@localhost:port/database
```

You can then call:

```bash
alembic upgrade head
```

## Example Usage

First, set up an account and subaccount at FTX.  Set up a corresponding API key that has trading permissions.  You then need to set up a secrets file that has the API key information.  For example, `.secrets/ftx_subaccount_1.json` may look something like this:

```json
{
    "API_KEY": "your_api_key",
    "API_SECRET": "your_secret_key",
    "SUBACCOUNT": "SUBACCOUNT_1"
}
```

Code in `scripts/ftx_static_cash_and_carry_perpetual.py` then provides example code for executing a static cash and carry trade.  

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

