const yaml = require("js-yaml");
const express = require("express");
const sanitize = require("htmlspecialchars");
const { pipeline } = require("stream/promises");
const gunzip = require("gunzip-maybe");
const stream = require("stream");
const tar = require("tar-stream");
const fs_p = require("fs/promises");
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
}

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
}

const get_pkgname = async (fn, db) =>
{
	for(const record of Object.values(db))
	{
		if(fn.startsWith(record.name))
		{
			return record.name;
		}
	}
}

const cleanup_db_pkg = async (files, pkgs, path) =>
{
	const keep = config.keep ?? 2;

	for(let i = keep; i < pkgs.length; i++)
	{
		const path_rm = path + "/" + pkgs[i][0];
		
		console.log("DELETE " + path_rm);

		delete files[pkgs[i]];

		try { await fs_p.rm(path_rm); } catch(e)
		{
			console.log("DELETE FAIL " + path_rm);
		}
	}
}

const cleanup_dbs = async (meta, db, name, vars) =>
{
	const path = "db/" + name + "/" + vars;
	const cmp = (a, b) => a[1] - b[1];

	for(let [pkgid, files] of Object.entries(meta))
	{
		let pkgs_sig = [];
		let pkgs_blob = [];

		for(let pkg of Object.entries(files))
		{
			if(pkg[0].endsWith(".sig")) pkgs_sig.push(pkg);
			else pkgs_blob.push(pkg);
		}

		pkgs_sig.sort(cmp);
		pkgs_blob.sort(cmp);

		await cleanup_db_pkg(files, pkgs_sig, path);
		await cleanup_db_pkg(files, pkgs_blob, path);
	}

	if(Object.keys(meta).length > 0 || fs.existsSync(path))
	{
		fs.writeFileSync(path + "/meta.json", JSON.stringify(meta));
	}
}

const read_dbs = async (dbs, meta, name, vars, path, mirrors, register=true) =>
{
	let pkgs_db = {};
	let promises = [];
	let failed = 0;

	for(let i = 0; i < mirrors.length; i++)
	{
		promises.push(read_db(name, path, mirrors[i], pkgs_db).catch((e) => {
			console.log("ERROR " + mirrors[i], e);
			failed++;
		}));
	}

	await Promise.all(promises);

	if(failed === promises.length)
	{
		return null;
	}

	let pkgs_db_fn = {};
	
	for(let item of Object.values(pkgs_db))
	{
		pkgs_db_fn[item.filename] = item;
	}

	if(register)
	{
		setInterval(async () =>
		{
			console.log("REFRESH " + name + "?" + vars);

			dbs[vars] = read_dbs(dbs, meta, name, vars, path, mirrors, false);

		}, (config.repos[name].refresh ?? 3600) * 1000);
	}

	await cleanup_dbs(meta, pkgs_db, name, vars);
	return pkgs_db_fn;
}

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
}

const get_dbs = async (dbs, all_meta, vars, name, path, mirrors) =>
{
	const db_path = "db/" + name + "/" + vars;
	
	if(!dbs[vars])
	{
		console.log("ADD " + name + "?" + vars);

		if(fs.existsSync(db_path) && fs.existsSync(db_path + "/meta.json"))
		{
			all_meta[vars] = JSON.parse(fs.readFileSync(db_path + "/meta.json"));
		}

		else
		{
			all_meta[vars] = {};
		}

		dbs[vars] = read_dbs(dbs, all_meta[vars], name, vars, path, mirrors);
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
	let bytes = params.get("bytes");

	if(!bytes) return {
		start: 0,
		end: Infinity,
	};
	
	let [start, end] = params.get("bytes").split("-");

	start = parseInt(start);
	end = parseInt(end);

	return {
		start: start || 0,
		end: isNaN(end) ? Infinity : end,
	};
}

const send_file_download = async (meta, item, name, vars, fs_path, fs_mode, rs, res) =>
{
	let file;
	
	if(fs_mode)
	{
		check_mkdir("db/" + name + "/" + vars);
		file = fs.createWriteStream(fs_path);
	}

	if(fs_mode && item)
	{
		if(!meta[item.name])
		{
			meta[item.name] = {};
		}

		meta[item.name][item.filename] = item.date;
	}

	for await (const chunk of rs)
	{
		if(!res.writable)
		{
			rs.destroy();

			if(!fs_mode) return;

			file.end();
			fs.rmSync(fs_path);
			
			if(!item) return;

			delete meta[item.name][item.filename];
			return;
		}
		
		if(fs_mode)
		{
			file.write(chunk);
		}

		res.write(chunk);
	}

	if(fs_mode)
	{
		file.end();
	}
	
	if(fs_mode && item)
	{
		fs.writeFileSync("db/" + name + "/" + vars + "/meta.json", JSON.stringify(meta));
	}

	res.send();
}

const pipe_to_client = (pipe, res) =>
{
	pipe.on("data", (chunk) =>
	{
		if(res.writable)
		res.write(chunk);

		else
		pipe.destroy();
	});

	pipe.on("end", () =>
	{
		res.send();
	});
}

const display_db = async (res, db, path, entrypoint, name, vars) =>
{
	let all = {};

	all[entrypoint] = {
		state: "Fresh",
	};

	for(const [pkgname, item] of Object.entries(db))
	{
		all[pkgname] = {
			state: "Fresh",
			mirror: item.mirror,
		};
		
		all[pkgname + ".sig"] = {
			state: "Fresh",
			mirror: item.mirror,
		};
	}

	const db_path = "db/" + name + "/" + vars;
	
	for(const pkgname of fs.existsSync(db_path) ? (await fs_p.readdir(db_path)) : [])
	{
		if(!all[pkgname])
		{
			all[pkgname] = {
				state: "Stale",
			};
		}

		else
		{
			all[pkgname].state = "Cached";
		}
	}

	let all_sorted = Object.entries(all).sort((a, b) => a[0].localeCompare(b[0]));
	let rows = [];

	for(let [pkgname, data] of all_sorted)
	{
		if(pkgname === "meta.json")
		{
			continue;
		}

		rows.push(`
<tr>
	<td><a href="/` + sanitize(name + "/" + path.join("/") + "/" + pkgname) + `">` + sanitize(pkgname) + `</a></td>
	<td>` + sanitize(data.state) + `</td>
	<td>` + (data.mirror ? (`<a href="` + sanitize(data.mirror + path.join("/")) + `" target="_blank">` + sanitize(data.mirror + path.join("/")) + `</a>`) : ``) + `</td>
</tr>
`);
	}

	res.header("Content-Type", "text/html");
	
	res.send(`
<!DOCTYPE html>
<html>
<head>

<title>Index of /` + sanitize(name + "/" + path.join("/")) + `</title>

<style>

td {
	padding: 0 1.5em;
}

</style>

</head>
<body>

<h1>Index of /` + sanitize(name + "/" + path.join("/")) + `</h1>

<table>

<tr>
	<th>Filename</th>
	<th>State</th>
	<th>Mirror</th>
</tr>

` + rows.join("\n") + `
</table>

</body>
</html>
`);
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

	let all_dbs = {};
	let all_meta = {};
	
	app.use("/" + name, async (req, res, next) =>
	{
		if(req.method !== "GET")
		{
			return res.status(400).send("Bad Method");
		}

		let path = req.path.split("/").filter(v => v);
		let vars = {};

		if(path.length - 1 !== path_t.length && path.length !== path_t.length)
		{
			return next();
		}

		if(path[path_t.length] === "meta.json")
		{
			return next();
		}
		
		for(let i = 0; i < path_t.length; i++)
		{
			let e = path_t[i];

			if(e[0] !== '$') continue;

			vars[e.substr(1)] = path[i];
		}

		let vars_u = new URLSearchParams(vars);
		let entrypoint = entrypoint_t;

		for(let [k, v] of Object.entries(vars))
		{
			vars_u.set(k, v);
			entrypoint = entrypoint.replaceAll("$" + k, v);
		}
		
		vars = vars_u.toString();

		const path_db = path.slice(0, path_t.length).concat(entrypoint);
		const db = await get_dbs(all_dbs, all_meta, vars, name, path_db, mirrors);

		if(!db)
		{
			return next();
		}

		if(path.length === path_t.length)
		{
			return display_db(res, db, path, entrypoint, name, vars);
		}

		const filename = path[path_t.length];
		const is_partial = req.headers.range && req.headers.range.length > 0;
		const filename_trimmed = filename.endsWith(".sig") ? filename.substr(0, filename.length - 4) : filename;

		if(filename === entrypoint)
		{
			let pack = tar.pack();

			res.header("Content-Type", "application/x-tar");
			
			pipe_to_client(pack, res);
			await create_db(db, pack);
			pack.finalize();

			return;
		}

		const item = db[filename_trimmed];
		const fs_path = "db/" + name + "/" + vars + "/" + path[path.length - 1];

		let rs;
		let fs_mode;
		
		if(fs.existsSync(fs_path))
		{
			let config = parse_client_range(req.headers.range);
			let stat = fs.statSync(fs_path);
			let start = config.start;
			let end = config.end;

			if(start > stat.size - 1) start = stat.size - 1;
			if(end > stat.size - 1) end = stat.size - 1;
			if(start !== 0 || end < stat.size) res.status(206);

			fs_mode = false;
			rs = fs.createReadStream(fs_path, config);
			console.log("HIT /" + name + "/" + path.join("/"));

			res.header("Accept-Ranges", "bytes");
			res.header("Content-Length", end - start + 1);
			res.header("Content-Type", "application/octet-stream");

			if(is_partial) 
			res.header("Content-Range", "bytes " + start + "-" + end + "/" + stat.size);
		}
		
		else if(!item)
		{
			return next();
		}

		else
		{
			const file_req = await fetch(item.mirror + req.path.substr(1), {
				headers: {
					range: req.headers.range,
				},
			});
			
			console.log("MISS " + item.mirror + path.join("/"));

			res.status(file_req.status);
			res.header("Accept-Ranges", file_req.headers.get("Accept-Ranges"));
			res.header("Content-Length", file_req.headers.get("Content-Length"));
			res.header("Content-Type", file_req.headers.get("Content-Type"));
			res.header("Content-Range", file_req.headers.get("Content-Range"));

			fs_mode = !is_partial;
			rs = file_req.body;
		}

		await send_file_download(all_meta[vars], item, name, vars, fs_path, fs_mode, rs, res);
	});
}

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

