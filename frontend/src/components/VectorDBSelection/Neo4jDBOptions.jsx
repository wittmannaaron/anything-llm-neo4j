import React from 'react';
import System from '@/models/system';
import { useTranslation } from 'react-i18next';

export default function Neo4jDBOptions({ settings }) {
  const { t } = useTranslation();

  return (
    <div className="w-full flex flex-col gap-y-4">
      <div className="w-full flex flex-col gap-y-2">
        <label className="text-white text-sm font-semibold">
          {t('vector.neo4j.uri')}
        </label>
        <input
          type="text"
          name="NEO4J_URI"
          required={true}
          defaultValue={settings?.NEO4J_URI}
          placeholder="bolt://localhost:7687"
          className="bg-zinc-900 text-white placeholder:text-white/20 text-sm rounded-lg focus:border-white block w-full p-2.5"
        />
      </div>

      <div className="w-full flex flex-col gap-y-2">
        <label className="text-white text-sm font-semibold">
          {t('vector.neo4j.username')}
        </label>
        <input
          type="text"
          name="NEO4J_USER"
          required={true}
          defaultValue={settings?.NEO4J_USER}
          placeholder="neo4j"
          className="bg-zinc-900 text-white placeholder:text-white/20 text-sm rounded-lg focus:border-white block w-full p-2.5"
        />
      </div>

      <div className="w-full flex flex-col gap-y-2">
        <label className="text-white text-sm font-semibold">
          {t('vector.neo4j.password')}
        </label>
        <input
          type="password"
          name="NEO4J_PASSWORD"
          required={true}
          defaultValue={settings?.NEO4J_PASSWORD}
          placeholder="••••••••"
          className="bg-zinc-900 text-white placeholder:text-white/20 text-sm rounded-lg focus:border-white block w-full p-2.5"
        />
      </div>
    </div>
  );
}