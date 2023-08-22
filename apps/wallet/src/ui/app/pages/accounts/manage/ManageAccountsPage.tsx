// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { AccountGroup } from './AccountGroup';

import Overlay from '../../../components/overlay';
import { useAccounts } from '../../../hooks/useAccounts';
import { type SerializedUIAccount } from '_src/background/accounts/Account';
import { useAccountSources } from '_src/ui/app/hooks/useAccountSources';

export function ManageAccountsPage() {
	const { data: accounts = [] } = useAccounts();

	const navigate = useNavigate();
	const { data: accountSources } = useAccountSources();

	const groupedAccounts = useMemo(() => {
		return accountSources?.reduce(
			(acc, source) => {
				acc[source.id] = accounts.filter((account) => account.type.includes(source.type));
				return acc;
			},
			{} as Record<string, SerializedUIAccount[]>,
		);
	}, [accounts, accountSources]);

	return (
		<Overlay showModal title="Manage Accounts" closeOverlay={() => navigate('/home')}>
			<div className="flex flex-col gap-4 flex-1">
				{accountSources?.map((source) => (
					<AccountGroup
						key={source.id}
						accountSource={source.id}
						accounts={groupedAccounts?.[source.id] || []}
						type={source.type}
					/>
				))}
			</div>
		</Overlay>
	);
}
