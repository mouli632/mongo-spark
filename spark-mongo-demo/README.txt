use dataspectre;
db.users_sample.insertMany(    [      { name: "bob", age: 42, status: "A", },      { name: "ahn", age: 22, status: "A", },      { name: "xi", age: 34, status: "D", }    ] );
db.users_sample.find()
