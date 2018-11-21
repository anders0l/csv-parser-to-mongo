require('dotenv-safe').config();
import csvToJson from 'csvtojson';
import { MongoClient } from 'mongodb';

const csvFilePath = './file-to-parse.csv';
const mongoUrl = process.env.MONGO_URL;
const dbName = process.env.DB_NAME;
const dbColletctionName = process.env.DB_COLLECTION_NAME;
const csvParams = {
	noheader: false,
	delimiter: ',',
	quote: '"',
	includeColumns: /^(id|oldWallet|icoBalance)$/
};

const updateUser = ({ client, oldId, dataToSend }) => {
	const db = client.db(dbName);
	const collection = db.collection(dbColletctionName);
	return new Promise((resolve, reject) =>
		collection.updateOne({ oldId }, { $set: dataToSend }, (err, user) => {
			if (err) {
				return reject(err);
			}
			if (!user.result.n) {
				return reject('This user is not in DB');
			}
			resolve(user);
		})
	);
};

new Promise((resolve, reject) => {
	MongoClient.connect(mongoUrl, { useNewUrlParser: true }, (err, client) => {
		if (err) {
			return reject(err);
		}
		console.log('Connected successfully to DB server');
		resolve(client);
	});
})
	.then(client => {
		return new Promise((resolve, reject) => {
			csvToJson(csvParams)
				.fromFile(csvFilePath)
				.subscribe((json) => {
					// console.log('json', json);
					const { id: oldId, oldWallet, icoBalance } = json;
					if (!oldId || !oldWallet || !icoBalance) {
						return reject('CSV not propetly parsed!');
					}
					const dataToSend = {
						oldWallet,
						icoBalance
					};
					console.log(`Processing User ID: ${oldId} ...`);
					return updateUser({ client, oldId, dataToSend })
						.then(() => {
							console.log(`Success User ID: ${oldId}!`);
						})
						.catch((err) => {
							console.error(`ERROR!!!! User ID: ${oldId}`, `Message: ${err}`);
						});
				})
				.then(() => {
					console.log('Completed!');
					resolve(client);
				});
		});
	})
	.then(client => client.close())
	.catch(err => {
		console.log(err);
	});
