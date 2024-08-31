export default function Neo4jDBOptions({ settings }) {
  return (
    <div className="w-full flex flex-col gap-y-7">
      <div className="w-full flex items-center gap-[36px] mt-1.5">
        <div className="flex flex-col w-60">
          <label className="text-white text-sm font-semibold block mb-3">
            Neo4j URI
          </label>
          <input
            type="url"
            name="Neo4jURI"
            className="bg-zinc-900 text-white placeholder:text-white/20 text-sm rounded-lg focus:outline-primary-button active:outline-primary-button outline-none block w-full p-2.5"
            placeholder="bolt://localhost:7687"
            defaultValue={settings?.Neo4jURI}
            required={true}
            autoComplete="off"
            spellCheck={false}
          />
        </div>

        <div className="flex flex-col w-60">
          <label className="text-white text-sm font-semibold block mb-3">
            Username
          </label>
          <input
            name="Neo4jUser"
            autoComplete="off"
            type="text"
            defaultValue={settings?.Neo4jUser}
            className="bg-zinc-900 text-white placeholder:text-white/20 text-sm rounded-lg focus:outline-primary-button active:outline-primary-button outline-none block w-full p-2.5"
            placeholder="neo4j"
          />
        </div>

        <div className="flex flex-col w-60">
          <label className="text-white text-sm font-semibold block mb-3">
            Password
          </label>
          <input
            name="Neo4jPassword"
            autoComplete="off"
            type="password"
            defaultValue={settings?.Neo4jPassword ? "*".repeat(20) : ""}
            className="bg-zinc-900 text-white placeholder:text-white/20 text-sm rounded-lg focus:outline-primary-button active:outline-primary-button outline-none block w-full p-2.5"
            placeholder="your-password"
          />
        </div>
      </div>
    </div>
  );
}
