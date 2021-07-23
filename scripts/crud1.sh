curl -X POST http://127.0.0.1:9121/Create/John/100;
curl -X POST http://127.0.0.1:9121/Create/Lizzie/100;
curl -X PUT http://127.0.0.1:9121/Transfer/Lizzie/John/50
curl -X GET http://127.0.0.1:9121/Balance/Lizzie
