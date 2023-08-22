// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { useResolveSuiNSName } from '@mysten/core';
import { formatAddress } from '@mysten/sui.js/utils';

import { type ReactNode } from 'react';
import { AccountItem } from './AccountItem';
import { LockUnlockButton } from './LockUnlockButton';
import { useActiveAddress } from '../../hooks';
import { useAccounts } from '../../hooks/useAccounts';

type AccountListItemProps = {
	address: string;
	icon?: ReactNode;
	handleLockAccount?(): void;
	handleUnlockAccount?(): void;
	editable?: boolean;
};

export function AccountListItem({
	address,
	icon,
	handleLockAccount,
	handleUnlockAccount,
}: AccountListItemProps) {
	const activeAddress = useActiveAddress();
	const { data: domainName } = useResolveSuiNSName(address);
	const { data: accounts } = useAccounts();
	const account = accounts?.find((account) => account.address === address);
	return (
		<AccountItem
			icon={icon}
			name={domainName ?? formatAddress(address)}
			selected={address === activeAddress}
			after={
				<div className="ml-auto">
					<div className="flex items-center justify-center">
						<LockUnlockButton
							isLocked={account?.isLocked ?? true}
							onClick={(e) => {
								e.stopPropagation();
								if (account?.isLocked) handleUnlockAccount?.();
								if (!account?.isLocked) handleLockAccount?.();
							}}
						/>
					</div>
				</div>
			}
			address={address!}
		/>
	);
}
