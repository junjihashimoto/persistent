{-# LANGUAGE FlexibleContexts, UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Persist.Zookeeper.PathPiece
    where

import Database.Persist
import Database.Persist.Zookeeper.Store
import Web.PathPieces (PathPiece (..))
import qualified Data.ByteString.Char8 as B
import qualified Data.Text as T
import qualified Data.ByteString.Base64.URL as B64


-- | ToPathPiece is used to convert a key to/from text
instance PathPiece (KeyBackend ZookeeperBackend entity) where
  fromPathPiece txt =
    case B64.decode $ B.pack $ T.unpack txt of
      Right v -> Just (Key (PersistText (T.pack $ B.unpack v)))
      Left _ -> Nothing
  toPathPiece (Key (PersistText txt)) = T.pack $ B.unpack $ B64.encode $ B.pack $ T.unpack txt
  toPathPiece (Key _) = error "Wrong key for pathpiece "

