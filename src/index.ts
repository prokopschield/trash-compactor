import assert from "assert";
import fs from "fs";
import { DonNode } from "@prokopschield/don";
import { contentType } from "mime-types";
import nsblob from "nsblob-native-if-available";
import path from "path";
import { cacheFn, Lock } from "ps-std";
import { uploadBuffer, uploadStream } from "v3cdn.nodesite.eu";

const stat_lock = new Lock();

export const stat = cacheFn(async (filename: string) => {
	const lock = await stat_lock.wait_and_lock();
	const stats = await fs.promises.lstat(filename).catch(() => undefined);

	lock.unlock();

	return stats;
});

const readdir_lock = new Lock();

export const readdir = cacheFn(async (dirname: string) => {
	const lock = await readdir_lock.wait_and_lock();
	const readdir = await fs.promises.readdir(dirname).catch(() => []);

	lock.unlock();

	return readdir.map((name) => path.resolve(dirname, name));
});

const upload_lock = new Lock();
const preupload_lock = new Lock();

export async function preupload(filename: string) {
	const ulock = upload_lock.lock();
	const plock = await preupload_lock.wait_and_lock();

	const readstream = fs.createReadStream(filename, {
		highWaterMark: 0x100000,
	});

	readstream.on("data", async (chunk) => {
		readstream.pause();
		await nsblob.store(chunk);
		readstream.resume();
	});

	readstream.on("end", () => {
		plock.unlock();
		ulock.unlock();
	});
}

export async function upload(filename: string) {
	preupload(filename);

	const lock = await upload_lock.wait_and_lock();
	const stats = await stat(filename);
	const stream = fs.createReadStream(filename);

	const url = await uploadStream(
		stream,
		path.basename(filename),
		contentType(path.extname(filename)) || "text/plain",
		stats?.mtime
	);

	lock.unlock();

	return url;
}

export async function process(filename: string): Promise<[string, URL]> {
	const basename = path.basename(filename);
	const stats = await stat(filename);

	if (stats?.isFile()) {
		return [basename, await upload(filename)];
	}

	if (stats?.isDirectory()) {
		const files = await readdir(filename);

		return [
			basename,
			await uploadBuffer(
				Buffer.from(
					new DonNode(
						await Promise.all(
							files.map(async (filename) => {
								const [name, url] = await process(filename);

								return [name, url.href] as [string, string];
							})
						)
					).fmt("\t")
				),
				basename,
				"text/plain",
				stats.mtime
			),
		];
	}

	return [
		basename,
		await uploadBuffer(
			Buffer.from(new DonNode(Object.entries(stats || [])).fmt("\t")),
			basename,
			"text/plain",
			stats?.mtime
		),
	];
}

const transaction_lock = new Lock();

export async function transaction(filename: string, dry = true) {
	const lock = await transaction_lock.wait_and_lock();

	const [name, url] = await process(filename);
	const directory = path.resolve(filename, "..");
	const donfile = path.resolve(directory, "compacted.don");

	const dondata = fs.existsSync(donfile)
		? await fs.promises.readFile(donfile)
		: "[]";

	const donhash = await nsblob.store(dondata);
	const don = DonNode.decode(String(dondata));

	don.set("__previous__", dondata === "[]" ? "" : donhash);
	don.set(name, url.pathname.slice(1));

	const new_dondata = don.fmt("\t") + "\n";
	const new_donhash = await nsblob.store(new_dondata);

	if (filename !== donfile && !dry) {
		await fs.promises.writeFile(donfile, new_dondata);
		await fs.promises.rm(filename, { recursive: true });
	}

	lock.release();

	return new_donhash;
}

export async function processSymbolicLink(name: string, dry = true) {
	const readlink = await fs.promises.readlink(name);
	const real = path.resolve(name, "..", readlink);
	const stats = await stat(real);

	if (!stats) {
		if (!dry) {
			await fs.promises.unlink(name);
			await fs.promises.writeFile(name, "Invalid symlink: " + readlink);
		}

		return;
	}

	const node = new DonNode([
		["type", "symlink"],
		["name", name],
		["hash", await transaction(real, true)],
		["target", readlink],
	]);

	const fmt = node.fmt("\t") + "\n";

	const hash = await uploadBuffer(
		Buffer.from(fmt),
		name,
		"text/plain",
		stats.mtimeMs
	);

	if (!dry) {
		await fs.promises.unlink(name);
		await fs.promises.writeFile(name, fmt);
	}

	return hash;
}

export async function compact(dirname: string, dry = true) {
	const entries = await readdir(dirname);

	return await Promise.all(
		entries.map(async (filename) => {
			try {
				const stats = await stat(filename);

				if (stats?.isDirectory()) {
					assert(await compact(filename, dry));
				} else if (stats?.isFile()) {
					preupload(filename);
				} else if (stats?.isSymbolicLink()) {
					await processSymbolicLink(filename, dry);
				}

				await transaction(filename, dry);
			} catch {
				return false;
			}
		})
	);
}
