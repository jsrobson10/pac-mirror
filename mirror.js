
const yaml = require("js-yaml");
const express = require("express");
const { pipeline } = require("stream/promises");
const gunzip = require("gunzip-maybe");
const stream = require("stream");
const tar = require("tar-stream");
const fs = require("fs");

const app = express();
const config = yaml.load(fs.readFileSync("config.yaml", "utf-8"));

const process_pkginfo = (lines) =>
{
	let pkginfo = {};
	let on_break = true;
	let values;
	let field;

	for(let line of lines)
	{
		if(line && on_break)
		{
			field = line;
			on_break = false;
			values = [];
		}

		else if(line)
		{
			values.push(line);
		}

		else if(!on_break)
		{
			pkginfo[field] = values.join("\n");
			on_break = true;
		}
	}

	return pkginfo;
};

const process_tar_chunk = (header, stream, next) => new Promise((res, rej) =>
{
	let chunks = [];

	if(header.type === "file")
	{
		stream.on("data", chunk => chunks.push(chunk));
	}

	stream.on("end", () =>
	{
		if(header.type === "file")
		{
			const lines = Buffer.concat(chunks).toString().split("\n");
			const pkginfo = process_pkginfo(lines);

			res(pkginfo);
		}

		next();
	});

	stream.resume();
});

const read_db = async (name, path, mirror, pkgs_db) =>
{
	const extract = tar.extract();
	const path_req = mirror + "/" + path.join("/");
	const req = await fetch(path_req, {});
	
	extract.on("entry", async (header, stream, next) =>
	{
		let pkginfo = await process_tar_chunk(header, stream, next);
		let pkg_date = parseInt(pkginfo["%BUILDDATE%"]);
		let pkg_fn = pkginfo["%FILENAME%"];
		let pkg_name = pkginfo["%NAME%"];
		let pkg_stored = pkgs_db[pkg_name];

		if(pkg_stored && pkg_stored.date >= pkg_date)
		{
			return;
		}

		pkgs_db[pkg_name] = {
			date: pkg_date,
			mirror: mirror,
			pkginfo: pkginfo,
			filename: pkg_fn,
			name: pkg_name,
			tarfn: header.name,
		};
	});
	
	await pipeline(req.body, gunzip(), extract);
};

const read_dbs = async (name, path, mirrors) =>
{
	let pkgs_db = {};
	let promises = [];

	for(let i = 0; i < mirrors.length; i++)
	{
		promises.push(read_db(name, path, mirrors[i], pkgs_db));
	}

	await Promise.all(promises);

	let pkgs_db_fn = {};
	
	for(let item of Object.values(pkgs_db))
	{
		pkgs_db_fn[item.filename] = item;
	}

	return pkgs_db_fn;
};

const create_db = async (pkgs_db, pack) =>
{
	for(let item of Object.values(pkgs_db))
	{
		let entry_data = [];

		for(let [field, value] of Object.entries(item.pkginfo))
		{
			entry_data.push(field);
			entry_data.push(value);
			entry_data.push("");
		}

		pack.entry({name: item.tarfn}, entry_data.join("\n"));
	}
};

const get_dbs = async (dbs, vars, name, path, mirrors) =>
{
	if(!dbs[vars])
	{
		console.log("added " + name + ":" + vars + " to be updated and downloaded");

		dbs[vars] = read_dbs(name, path, mirrors);

		setInterval(async () =>
		{
			console.log("redownloading " + name + ":" + vars);

			dbs[vars] = read_dbs(name, path, mirrors);

		}, (config.repos[name].refresh ?? 3600) * 1000);
	}

	return await dbs[vars];
}

const check_mkdir = (path) =>
{
	if(!fs.existsSync(path))
	{
		fs.mkdirSync(path);
	}
}

const parse_client_range = (v) =>
{
	let params = new URLSearchParams(v);
	let [start, end] = params.get("bytes").split("-");

	return {
		start: parseInt(start) || 0,
		end: parseInt(end) ?? Infinity
	};
}

const send_file_download = async (name, vars, item, is_sig, is_partial, req, res) =>
{
	const path = "db/" + name + "/" + vars + "/" + item.name;
	const fn = is_sig ? "/sig" : "/blob";
	const fs_mode = !is_partial;
	
	let file;
	
	if(fs_mode)
	{
		check_mkdir("db/" + name + "/" + vars);
		check_mkdir(path);

		fs.writeFileSync(path + fn + ".version", item.pkginfo["%VERSION%"]);
		file = fs.createWriteStream(path + fn);
	}
	
	for await (const chunk of req.body)
	{
		if(!res.writable)
		{
			if(!fs_mode) return;

			file.end();
			fs.rmSync(path + fn);

			return;
		}
		
		if(fs_mode)
		file.write(chunk);

		res.write(chunk);
	}

	if(fs_mode)
	file.end();

	res.send();
}

/*
 * each repository path item will contain any number of variables,
 * which will each begin with a dollar sign '$'. variables may
 * also be used to specify the entrypoint file (eg '$repo.db').
 */
const repo = (name, path_t, mirrors, entrypoint_t) =>
{
	for(let i = 0; i < mirrors.length; i++)
	{
		mirrors[i] = mirrors[i].replaceAll("$name", name);
	}
	
	path_t = path_t.split("/").filter(v => v);

	console.log("will use " + name + " for " + JSON.stringify(path_t) + ", " + JSON.stringify(mirrors) + ", and " + entrypoint_t);
	
	let all_dbs = {};
	
	app.use("/" + name, async (req, res) =>
	{
		if(req.method !== "GET")
		{
			res.status(400).send("Bad Method");
			return;
		}

		let path = req.path.split("/").filter(v => v);
		let vars = {};

		if(path.length - 1 !== path_t.length)
		{
			res.status(404).send("Not Found");
			return;
		}
		
		let filename = path[path_t.length];

		for(let i = 0; i < path_t.length; i++)
		{
			let e = path_t[i];

			if(e[0] !== '$') continue;

			vars[e.substr(1)] = path[i];
		}

		let path_db = Array.from(path);
		let vars_u = new URLSearchParams(vars);
		let entrypoint = entrypoint_t;

		for(let [k, v] of Object.entries(vars))
		{
			vars_u.set(k, v);
			entrypoint = entrypoint.replaceAll("$" + k, v);
		}
		
		path_db[path_t.length] = entrypoint;
		vars = vars_u.toString();

		const db = await get_dbs(all_dbs, vars, name, path_db, mirrors);
		const is_partial = req.headers.range && req.headers.range.length > 0;
		const is_sig = filename.endsWith(".sig");

		if(is_sig)
		{
			filename = filename.substr(0, filename.length - 4);
		}

		else if(filename === entrypoint)
		{
			let pack = tar.pack();

			res.header("Content-Type", "application/x-tar");
			pack.pipe(res);

			await create_db(db, pack);

			pack.finalize();

			return;
		}

		const item = db[filename];

		if(!item)
		{
			res.status(404).send("Not Found");
			return;
		}

		const fs_path = "db/" + name + "/" + vars + "/" + item.name;
		const fs_fn = is_sig ? "/sig" : "/blob";

		let rs;
		
		if(fs.existsSync(fs_path + fs_fn) && fs.existsSync(fs_path + fs_fn + ".version") && fs.readFileSync(fs_path + fs_fn + ".version") === item.pkginfo["%VERSION%"])
		{
			let config = get_start_end(req.headers.range);
			rs = fs.createReadStream(fs_path + fs_fn, config);

			let start = config.start;
			let end = config.end;

			if(start > stat.size) start = stat.size;
			if(end > stat.size) start = stat.size;
			if(start !== 0 || end >= stat.size) res.status(206);

			console.log("HIT /" + name + req.path);

			res.header("Accept-Ranges", "bytes");
			res.header("Content-Length", stat.size);
			res.header("Content-Range", "bytes " + start + "-" + end + "/" + stat.size);
			res.header("Content-Type", "application/octet-stream");
		}

		else
		{
			const file_req = await fetch(item.mirror + req.path.substr(1), {
				headers: {
					range: req.headers.range,
				},
			});
			
			console.log("MISS " + item.mirror + name + req.path);

			res.status(file_req.status);
			res.header("Accept-Ranges", file_req.headers.get("Accept-Ranges"));
			res.header("Content-Length", file_req.headers.get("Content-Length"));
			res.header("Content-Range", file_req.headers.get("Content-Range"));
			res.header("Content-Type", file_req.headers.get("Content-Type"));
			rs = file_req;
		}

		await send_file_download(name, vars, item, is_sig, is_partial, rs, res);
	});
};

check_mkdir("db");

for(let [name, r_config] of Object.entries(config.repos))
{
	check_mkdir("db/" + name);
	repo(name, r_config.path, Array.from(config.mirrors[r_config.mirrors]), r_config.entrypoint);
}

app.listen(config["port"], () =>
{
	console.log("listening on port " + config["port"]);
});

