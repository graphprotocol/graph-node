use alloy::consensus::BlobTransactionSidecarVariant;
use alloy::eips::eip7702::SignedAuthorization;
use alloy::network::{
    AnyHeader, AnyReceiptEnvelope, AnyRpcTransaction, AnyTxEnvelope, AnyTxType,
    AnyTypedTransaction, BuildResult, Network, NetworkTransactionBuilder, NetworkWallet,
    TransactionBuilder, TransactionBuilder4844, TransactionBuilder7702, TransactionBuilderError,
};
use alloy::primitives::{Address, Bytes, ChainId, TxKind, U256};
use alloy::providers::fillers::{
    BlobGasFiller, ChainIdFiller, GasFiller, JoinFill, NonceFiller, RecommendedFillers,
};
use alloy::rpc::types::{
    AccessList, Block, Header, Log, Transaction, TransactionInputKind, TransactionReceipt,
    TransactionRequest,
};
use alloy::serde::WithOtherFields;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

use super::types::AnyTransaction;

/// Like alloy's [`AnyNetwork`] but without [`WithOtherFields`] on response types.
///
/// `AnyNetwork` wraps every response (block, transaction, receipt) in `WithOtherFields<T>`,
/// which captures unknown JSON keys into a `BTreeMap`. This requires `#[serde(flatten)]`,
/// forcing serde to buffer entire JSON objects into intermediate `Value` maps and
/// re-serialize them to diff keys — on every block and every transaction.
/// graph-node never uses these extra fields, so we define a bare network that deserializes
/// directly into the inner types.
///
/// Uses `AnyTxEnvelope` (not `TxEnvelope`) to support non-standard transaction types from
/// L2s and sidechains. `TransactionRequest` retains `WithOtherFields` because upstream
/// `From` impls for `AnyTxEnvelope` only exist on `WithOtherFields<TransactionRequest>`.
///
/// [`AnyNetwork`]: alloy::network::AnyNetwork
/// [`WithOtherFields`]: alloy::serde::WithOtherFields
#[derive(Clone, Copy, Debug)]
pub struct AnyNetworkBare;

/// Newtype around `WithOtherFields<TransactionRequest>` needed because
/// `Network::TransactionRequest` requires `From<Self::TransactionResponse>` and our
/// `TransactionResponse` is `Transaction<AnyTxEnvelope>` (not wrapped in `WithOtherFields`).
/// Upstream only provides `From<WithOtherFields<Transaction<AnyTxEnvelope>>>`, so we need
/// this local type to bridge the conversion. Additionally, `NetworkTransactionBuilder`
/// requires `TransactionBuilder + TransactionBuilder4844 + TransactionBuilder7702` as
/// supertraits, so we must re-implement those by delegation since Rust doesn't auto-delegate
/// trait impls through `Deref`.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(transparent)]
pub struct AnyTransactionRequest(WithOtherFields<TransactionRequest>);

impl Deref for AnyTransactionRequest {
    type Target = WithOtherFields<TransactionRequest>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for AnyTransactionRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<AnyTxEnvelope> for AnyTransactionRequest {
    fn from(value: AnyTxEnvelope) -> Self {
        Self(value.into())
    }
}

impl From<TransactionRequest> for AnyTransactionRequest {
    fn from(value: TransactionRequest) -> Self {
        Self(value.into())
    }
}

impl From<AnyTypedTransaction> for AnyTransactionRequest {
    fn from(value: AnyTypedTransaction) -> Self {
        Self(value.into())
    }
}

impl From<AnyTransaction> for AnyTransactionRequest {
    fn from(value: AnyTransaction) -> Self {
        // Upstream only has From<AnyRpcTransaction> for WithOtherFields<TransactionRequest>,
        // so we wrap in WithOtherFields first to match that impl.
        let tx = WithOtherFields::<Transaction<AnyTxEnvelope>>::new(value);
        Self(AnyRpcTransaction::from(tx).into())
    }
}

impl Network for AnyNetworkBare {
    type TxType = AnyTxType;
    type TxEnvelope = AnyTxEnvelope;
    type UnsignedTx = AnyTypedTransaction;
    type ReceiptEnvelope = AnyReceiptEnvelope;
    type Header = AnyHeader;

    type TransactionRequest = AnyTransactionRequest;
    type TransactionResponse = AnyTransaction;
    type ReceiptResponse = TransactionReceipt<AnyReceiptEnvelope<Log>>;
    type HeaderResponse = Header<AnyHeader>;
    type BlockResponse = Block<AnyTransaction, Header<AnyHeader>>;
}

impl RecommendedFillers for AnyNetworkBare {
    type RecommendedFillers =
        JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>;

    fn recommended_fillers() -> Self::RecommendedFillers {
        Default::default()
    }
}

// Delegate `TransactionBuilder` to the inner `WithOtherFields<TransactionRequest>`.
// Required as a supertrait of `NetworkTransactionBuilder`.
impl TransactionBuilder for AnyTransactionRequest {
    fn chain_id(&self) -> Option<ChainId> {
        self.0.chain_id()
    }

    fn set_chain_id(&mut self, chain_id: ChainId) {
        self.0.set_chain_id(chain_id)
    }

    fn nonce(&self) -> Option<u64> {
        self.0.nonce()
    }

    fn set_nonce(&mut self, nonce: u64) {
        self.0.set_nonce(nonce)
    }

    fn take_nonce(&mut self) -> Option<u64> {
        self.0.deref_mut().nonce.take()
    }

    fn input(&self) -> Option<&Bytes> {
        self.0.input()
    }

    fn set_input<T: Into<Bytes>>(&mut self, input: T) {
        self.0.set_input(input);
    }

    fn set_input_kind<T: Into<Bytes>>(&mut self, input: T, kind: TransactionInputKind) {
        self.0.set_input_kind(input, kind)
    }

    fn from(&self) -> Option<Address> {
        self.0.from()
    }

    fn set_from(&mut self, from: Address) {
        self.0.set_from(from);
    }

    fn kind(&self) -> Option<TxKind> {
        self.0.kind()
    }

    fn clear_kind(&mut self) {
        self.0.clear_kind()
    }

    fn set_kind(&mut self, kind: TxKind) {
        self.0.set_kind(kind)
    }

    fn value(&self) -> Option<U256> {
        self.0.value()
    }

    fn set_value(&mut self, value: U256) {
        self.0.set_value(value)
    }

    fn gas_price(&self) -> Option<u128> {
        self.0.gas_price()
    }

    fn set_gas_price(&mut self, gas_price: u128) {
        self.0.set_gas_price(gas_price);
    }

    fn max_fee_per_gas(&self) -> Option<u128> {
        self.0.max_fee_per_gas()
    }

    fn set_max_fee_per_gas(&mut self, max_fee_per_gas: u128) {
        self.0.set_max_fee_per_gas(max_fee_per_gas);
    }

    fn max_priority_fee_per_gas(&self) -> Option<u128> {
        self.0.max_priority_fee_per_gas()
    }

    fn set_max_priority_fee_per_gas(&mut self, max_priority_fee_per_gas: u128) {
        self.0
            .set_max_priority_fee_per_gas(max_priority_fee_per_gas);
    }

    fn gas_limit(&self) -> Option<u64> {
        self.0.gas_limit()
    }

    fn set_gas_limit(&mut self, gas_limit: u64) {
        self.0.set_gas_limit(gas_limit);
    }

    fn access_list(&self) -> Option<&AccessList> {
        self.0.access_list()
    }

    fn set_access_list(&mut self, access_list: AccessList) {
        self.0.set_access_list(access_list)
    }
}

// Delegate `TransactionBuilder4844` — required as a supertrait of `NetworkTransactionBuilder`.
impl TransactionBuilder4844 for AnyTransactionRequest {
    fn max_fee_per_blob_gas(&self) -> Option<u128> {
        self.0.max_fee_per_blob_gas()
    }

    fn set_max_fee_per_blob_gas(&mut self, max_fee_per_blob_gas: u128) {
        self.0.set_max_fee_per_blob_gas(max_fee_per_blob_gas)
    }

    fn blob_sidecar(&self) -> Option<&BlobTransactionSidecarVariant> {
        self.0.blob_sidecar()
    }

    fn has_blob_sidecar(&self) -> bool {
        self.0.has_blob_sidecar()
    }

    fn set_blob_sidecar(&mut self, sidecar: BlobTransactionSidecarVariant) {
        self.0.set_blob_sidecar(sidecar)
    }
}

// Delegate `TransactionBuilder7702` — required as a supertrait of `NetworkTransactionBuilder`.
impl TransactionBuilder7702 for AnyTransactionRequest {
    fn authorization_list(&self) -> Option<&Vec<SignedAuthorization>> {
        self.0.authorization_list()
    }

    fn set_authorization_list(&mut self, authorization_list: Vec<SignedAuthorization>) {
        self.0.set_authorization_list(authorization_list)
    }
}

impl NetworkTransactionBuilder<AnyNetworkBare> for AnyTransactionRequest {
    fn complete_type(
        &self,
        ty: <AnyNetworkBare as Network>::TxType,
    ) -> Result<(), Vec<&'static str>> {
        self.0
            .deref()
            .complete_type(ty.try_into().map_err(|_| vec!["supported tx type"])?)
    }

    fn can_submit(&self) -> bool {
        self.0.deref().can_submit()
    }

    fn can_build(&self) -> bool {
        self.0.deref().can_build()
    }

    fn output_tx_type(&self) -> <AnyNetworkBare as Network>::TxType {
        self.0.deref().output_tx_type().into()
    }

    fn output_tx_type_checked(&self) -> Option<<AnyNetworkBare as Network>::TxType> {
        self.0.deref().output_tx_type_checked().map(Into::into)
    }

    fn prep_for_submission(&mut self) {
        self.0.deref_mut().prep_for_submission()
    }

    fn build_unsigned(
        self,
    ) -> BuildResult<<AnyNetworkBare as Network>::UnsignedTx, AnyNetworkBare> {
        if let Err((tx_type, missing)) = self.0.missing_keys() {
            return Err(TransactionBuilderError::InvalidTransactionRequest(
                tx_type.into(),
                missing,
            )
            .into_unbuilt(self));
        }
        Ok(self
            .0
            .inner
            .build_typed_tx()
            .expect("checked by missing_keys")
            .into())
    }

    async fn build<W: NetworkWallet<AnyNetworkBare>>(
        self,
        wallet: &W,
    ) -> Result<<AnyNetworkBare as Network>::TxEnvelope, TransactionBuilderError<AnyNetworkBare>>
    {
        Ok(wallet.sign_request(self).await?)
    }
}
